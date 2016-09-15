#include <iostream>
#include <array>
#include <vector>
#include <queue>
#include <random>
#include <atomic>
#include <thread>


#ifdef BENCHMARK
#include "wrappers.h"
#endif

#ifdef NO_INLINE
#define HINLINE __attribute__((noinline))
#else
#define HINLINE inline
#endif

#define likely(x) __builtin_expect(!!(x), 1)


template<typename T>
class lf_queue
{
private:

	template<typename Ptr, int N> struct aligned_atomic_ptr;

	static constexpr int alignment  = 64;
	static constexpr int num_queues = 32;
	static constexpr int queue_mask = num_queues - 1;

	using underlying_type = std::queue<T>;
	using pointer  		  = underlying_type*;
	using atomic_pointer  = aligned_atomic_ptr<pointer, alignment>;
	using container 	  = std::array<atomic_pointer, num_queues>;


	/* 
	Implements a coupling between an atomic pointer and associated
	   bool. The bool is a lazy predictor regarding the "dirtyness" of the
	   underlying queue. If inidcated as true, the thread can optimistically
	   perform the CAS, check the queue, and proceed. This potentially saves
	   it several iterations of useless CAS only to have to return the pointer
	   because the underlying queue was actually empty. 

	   A future implementation would hold these two together in a union.
	   Thus, it would only require a single atomic load and store of the union
	   to check and set the dirtyness of the queue. Given the upper 16 bits of a 64 bit pointer
	   or the lower 8 bits due to an allocation from, e.g. malloc(), this bool can 
	   be embedded without causing problems.

	*/

	template<typename Ptr, std::size_t Alignment>
    struct alignas(Alignment) aligned_atomic_ptr
	{
		std::atomic<Ptr> atomic_ptr;
		alignas(alignment) std::atomic<bool> dirty_; 

		aligned_atomic_ptr() : dirty_(false) {}

		template<typename U>
		void store(U&& data, std::memory_order tag = std::memory_order_seq_cst)
		{
			atomic_ptr.store(std::forward<U>(data), tag);
		}
		Ptr load(std::memory_order tag = std::memory_order_seq_cst)
		{
			return atomic_ptr.load(tag);
		}

		bool compare_exchange_weak(Ptr& expec, Ptr desired, std::memory_order success, std::memory_order failure)
		{
			return atomic_ptr.compare_exchange_weak(expec, desired, success, failure);
		}

		bool compare_exchange_strong(Ptr& expec, Ptr desired, std::memory_order success, std::memory_order failure)
		{
			return atomic_ptr.compare_exchange_strong(expec, desired, success, failure);
		}

	};

	container data_;
	std::atomic<int> thread_offset;

public:

#ifdef BENCHMARK
	using consumer_token_t = DummyToken;
	using producer_token_t = DummyToken;

	bool enqueue(producer_token_t const&, T const&) { return false; }
	bool try_enqueue(producer_token_t, T const&) { return false; }
	bool try_dequeue(consumer_token_t, T& item) { return false; }
	template<typename It> bool enqueue_bulk(producer_token_t const&, It, size_t) { return false; }

	template<typename It> size_t try_dequeue_bulk(consumer_token_t, It, size_t) { return 0; }
#endif

	lf_queue() : thread_offset(0) 
	{
		for(int i = 0 ; i < num_queues; ++i)
		 {
		 		data_[i].store( new underlying_type{}, std::memory_order_relaxed );
		 }
	}

	struct queue_holder
	{
		container& data_;
		int index_;
		pointer ptr_;
	
		queue_holder(int idx, pointer p, container& init) : data_(init), index_(idx), ptr_(p) {}
		queue_holder(queue_holder&& other) : data_(other.data_), index_(other.index_), ptr_(other.ptr_)
		{
			other.ptr_ = nullptr;
		}
		queue_holder& operator=(queue_holder&& other)
		{
			
			data_[index_].store(ptr_, std::memory_order_release); // release current pointer
			index_ = other.index_;
			ptr_ = other.ptr_;
			other.ptr_ = nullptr;

			return *this;
		}
		queue_holder& operator=(const queue_holder&) = delete;

		underlying_type& queue()
		{
			return *ptr_;
		}
		~queue_holder()
		{
			if(ptr_)
			{
				data_[index_].dirty_.store(! queue().empty(), std::memory_order_relaxed);
				data_[index_].store(ptr_, std::memory_order_release);
			}
		}
	};

	HINLINE unsigned int get_index()
	{	

		
		static thread_local unsigned int cpuid = sched_getcpu() * 4 ;
		static thread_local int countdown = 500;
		static thread_local int local_offset = thread_offset.fetch_add(1, std::memory_order_relaxed) & 3;
		

		if(countdown-- > 0)
			return cpuid + local_offset;

		countdown = 500;
		__rdtscp(&cpuid);
		cpuid *= 4;
		return cpuid + local_offset;
		
	}

	unsigned int query_cpu()
 	{
 		unsigned int cpuid;
 		__rdtscp(&cpuid);
 		return cpuid;
 	}

	
	HINLINE queue_holder acquire_queue_dequeue()
	{
		
		
		unsigned int starting_position = get_index();
		for(int i = 0; i < num_queues; ++i)
		{
			int index = (starting_position + i) & queue_mask; 
			
			if(! data_[index].dirty_.load(std::memory_order_relaxed))
				continue;

			pointer ptr = data_[index].load(std::memory_order_relaxed);
			if(ptr &&  data_[index].compare_exchange_weak(ptr, nullptr, std::memory_order_acquire, std::memory_order_relaxed))
			{	
				return {index, ptr, data_};
			}
			
		}
		return {0, nullptr, data_};
	}
 
 
	HINLINE queue_holder acquire_queue()
	{
		
		
		unsigned int starting_position = get_index();

		for(int i = 0;; ++i)
		{
			int index = (starting_position + i) & queue_mask; // why wasn't the queue_mask calculation done outside of the loop?
			pointer ptr = data_[index].load(std::memory_order_relaxed);

			if( ptr && data_[index].compare_exchange_weak(ptr, nullptr, std::memory_order_acquire, std::memory_order_relaxed))
			{
				
				return {index, ptr, data_};
			}
			
		}
	}

	
	template<typename U>
	HINLINE bool enqueue(U&& arg)
	{
		auto queue_guard =  acquire_queue(); // atmically acquire queue
		auto& queue = queue_guard.queue(); // get a reference to the queue

		queue.emplace(std::forward<U>(arg));
		return true;
	}

	HINLINE bool enqueue(const T& copy)
	{
		auto queue_guard =  acquire_queue(); // atmically acquire queue
		auto& queue = queue_guard.queue(); // get a reference to the queue

		queue.push(copy);
		return true;
	}

	HINLINE  bool enqueue(T&& tmp)
	{
		auto queue_guard =  acquire_queue(); // atmically acquire queue
		auto& queue = queue_guard.queue(); // get a reference to the queue

		queue.push(std::move(tmp));
		return true;
	}

	template<typename It>
	bool enqueue_bulk(It iter, std::size_t count)
	{
		auto queue_guard =  acquire_queue(); // atmically acquire queue
		auto& queue = queue_guard.queue(); // get a reference to the queue


		for(std::size_t i = 0; i < count; ++i)
		{
			queue.push(*(iter++));
		}
		return true;
	}

	HINLINE bool try_dequeue(T& item)
	{

		
		int iters = 0;
		for(;;)
		{
			auto guard = acquire_queue_dequeue();
			if(guard.ptr_ == nullptr)
				return false;
			auto& queue = guard.queue();
			if(!queue.empty())
			{
				item = std::move(queue.front());
				queue.pop();
				return true;	
			}
			if(iters++ == num_queues)
				return false; // iterated all buckets, found nothing
		}

		
	}

	template<typename It>
	size_t try_dequeue_bulk(It output, size_t items)
	{
		auto queue_guard = acquire_queue_dequeue();
		if(queue_guard.ptr_ == nullptr)
			return 0;
		auto* queue = &queue_guard.queue();
		size_t count = 0;
		int    iters = 0;
		while(count < items)
		{
			std::size_t add = std::min(queue->size(), items - count);
			count += add;
			for(std::size_t i = 0; i < add; ++i)
			{
				*(output++) = std::move(queue->front());
				queue->pop();
			}
			if(count == items || iters++ == num_queues) // searched through enough iterations or reached goal
				return count;
			queue_guard = acquire_queue_dequeue();
			queue = &queue_guard.queue();

		}

		return count;

	}

	


	void debug()
	{
		std::cout << "##########LF QUEUE INTERNALS##########\n";
		int width = 200;
		int item_count = 0;
		for(int i = 0; i < num_queues; ++i)
		{
					std::cout.width(width);
					std::cout << "#\tQueue " << i << " contents:\n" << std::right << "#\n";
					auto queue_ptr = data_[i].load(std::memory_order_relaxed);
					item_count += queue_ptr->size();
					while(!queue_ptr->empty())
					{
						std::cout.width(width);
						std::cout << "#\t" << queue_ptr->front() << std::right << "#\n";
						queue_ptr->pop();
					}
					std::cout << '\n';
		}
		std::cout << "\n\n" << "queue contains: " << item_count << " items\n";
		

	}

	

	~lf_queue()
	{
		for(auto& st_queue : data_)
		{
			delete st_queue.load(std::memory_order_relaxed);
		}
	}





};
