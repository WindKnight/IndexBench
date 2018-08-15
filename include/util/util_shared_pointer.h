/*
 * gcache_shared_pointer.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef SEISBASE_SHARED_POINTER_H_
#define SEISBASE_SHARED_POINTER_H_

#include <exception>
#include "util/util_log.h"


struct NullPointerException: public std::exception {

	NullPointerException() throw () {
	}

	~NullPointerException() throw () {
	}

	const char* what() const throw () {
		return "[Yasper Exception] Attempted to dereference null pointer";
	}
};

struct Counter {

	Counter(unsigned c = 1) :
			count(c), del(true) {
	}
	unsigned count;
	//GAtomicInt count;
	volatile bool del;
};

template<typename X>
class SharedPtr {
public:
	typedef X element_type;

	/*
	 SharedPtr needs to be its own friend so SharedPtr< X > and SharedPtr< Y > can access
	 each other's private data members
	 */
	template<class Y> friend class SharedPtr;

	/*
	 default constructor
	 - don't create Counter
	 */
	SharedPtr() :
			rawPtr(0), counter(0) {
	}

	/*
	 Construct from a raw pointer
	 */
	SharedPtr(X* raw, Counter* c = 0) :
			rawPtr(0), counter(0) {
		if (raw) {
			rawPtr = raw;
			if (c)
				acquire(c);
			else
				counter = new Counter;
		}
	}

	template<typename Y>
	explicit SharedPtr(Y* raw, Counter* c = 0) :
			rawPtr(0), counter(0) {
		if (raw) {
			rawPtr = static_cast<X*>(raw);
			if (c)
				acquire(c);
			else
				counter = new Counter;
		}
	}

	/*
	 Copy constructor
	 */
	SharedPtr(const SharedPtr<X>& otherPtr) {
		acquire(otherPtr.counter);
		rawPtr = otherPtr.rawPtr;
	}

	template<typename Y>
	explicit SharedPtr(const SharedPtr<Y>& otherPtr) :
			rawPtr(0), counter(0) {
		acquire(otherPtr.counter);
		rawPtr = static_cast<X*>(otherPtr.getRawPointer());
	}

	/*
	 Destructor
	 */
	~SharedPtr() {
		decrement();
	}

	/*
	 Assignment to another SharedPtr
	 */

	SharedPtr& operator=(const SharedPtr<X>& otherPtr) {
		if (this != &otherPtr) {
			decrement();
			acquire(otherPtr.counter);
			rawPtr = otherPtr.rawPtr;
		}
		return *this;
	}

	template<typename Y>
	SharedPtr& operator=(const SharedPtr<Y>& otherPtr) {
		if (this != (SharedPtr<X>*) &otherPtr) {
			decrement();
			acquire(otherPtr.counter);
			rawPtr = static_cast<X*>(otherPtr.getRawPointer());
		}
		return *this;
	}

	/*
	 Assignment to raw pointers is really dangerous business.
	 If the raw pointer is also being used elsewhere,
	 we might prematurely delete it, causing much pain.
	 Use sparingly/with caution.
	 */

	SharedPtr& operator=(X* raw) {
		decrement();
		if (raw) {
			counter = new Counter;
			rawPtr = raw;
		} else {
			counter = 0;
			rawPtr = 0;
		}
		return *this;
	}

	template<typename Y>
	SharedPtr& operator=(Y* raw) {
		if (raw) {
			decrement();
			counter = new Counter;
			rawPtr = static_cast<X*>(raw);
		}
		return *this;
	}

	/*
	 assignment to long to allow SharedPtr< X > = NULL,
	 also allows raw pointer assignment by conversion.
	 Raw pointer assignment is really dangerous!
	 If the raw pointer is being used elsewhere,
	 it will get deleted prematurely.
	 */
	SharedPtr& operator=(long num) {
		if (num == 0) //pointer set to null
				{
			decrement();
		} else //assign raw pointer by conversion
		{
			decrement();
			counter = new Counter;
			rawPtr = reinterpret_cast<X*>(num);
		}

		return *this;
	}

	/*
	 Member Access
	 */
	X* operator->() const {
		return getRawPointer();
	}

	/*
	 Dereference the pointer
	 */
	X& operator*() const {
		return *getRawPointer();
	}

	/*
	 Conversion/casting operators
	 */

	operator bool() const {
		return isValid();
	}

	/*
	 implicit casts to base types of the
	 the pointer we're storing
	 */

	template<typename Y>
	operator Y*() const {
		return static_cast<Y*>(rawPtr);
	}

	template<typename Y>
	operator const Y*() const {
		return static_cast<const Y*>(rawPtr);
	}

	template<typename Y>
	operator SharedPtr<Y>() {
		//new SharedPtr must also take our counter or else the reference counts
		//will go out of sync
		return SharedPtr<Y>(rawPtr, counter);
	}

	/*
	 Provide access to the raw pointer
	 */

	X* getRawPointer() const {
		if (rawPtr == 0)
			throw new NullPointerException;
		return rawPtr;
	}

	X* get() const {
		return rawPtr;
	}

	X* release() {
		if (counter) {
			counter->del = false;
		}

		//(counter->count).fetchAndStoreRelaxed(0);
		return rawPtr;
	}

	/*
	 Is there only one reference on the counter?
	 */
	bool isUnique() const {
		if (counter && counter->count == 1)
			return true;
		return false;
	}

	bool isValid() const {
		if (counter && rawPtr)
			return true;
		return false;
	}

	unsigned getCount() const {
		if (counter)
			return counter->count;
		return 0;
	}

private:
	X* rawPtr;

	Counter* counter;

	// increment the count

	void acquire(Counter* c) {
		counter = c;
		if (c) {
			(c->count)++;
		}
	}

	// decrement the count, delete if it is 0

	void decrement() {
		if (counter) {
			//(counter->count)--;
			//int count = (counter->count).fetchAndAddRelaxed(-1);
			if (!(--(counter->count))) {
				if (counter->del)
					delete rawPtr;
				delete counter;
			}
		}
		counter = 0;
		rawPtr = 0;

	}
};

template<typename X, typename Y>
bool operator==(const SharedPtr<X>& lptr, const SharedPtr<Y>& rptr) {
	return lptr.getRawPointer() == rptr.getRawPointer();
}

template<typename X, typename Y>
bool operator==(const SharedPtr<X>& lptr, Y* raw) {
	return lptr.getRawPointer() == raw;
}

template<typename X>
bool operator==(const SharedPtr<X>& lptr, long num) {
	if (num == 0 && !lptr.isValid()) //both pointer and address are null
			{
		return true;
	} else //convert num to a pointer, compare addresses
	{
		return lptr == reinterpret_cast<X*>(num);
	}

}

template<typename X, typename Y>
bool operator!=(const SharedPtr<X>& lptr, const SharedPtr<Y>& rptr) {
	return (!operator==(lptr, rptr));
}

template<typename X, typename Y>
bool operator!=(const SharedPtr<X>& lptr, Y* raw) {
	return (!operator==(lptr, raw));
}

template<typename X>
bool operator!=(const SharedPtr<X>& lptr, long num) {
	return (!operator==(lptr, num));
}

template<typename X, typename Y>
bool operator&&(const SharedPtr<X>& lptr, const SharedPtr<Y>& rptr) {
	return lptr.isValid() && rptr.isValid();
}

template<typename X>
bool operator&&(const SharedPtr<X>& lptr, bool rval) {
	return lptr.isValid() && rval;
}

template<typename X>
bool operator&&(bool lval, const SharedPtr<X>& rptr) {
	return lval && rptr.isValid();
}

template<typename X, typename Y>
bool operator||(const SharedPtr<X>& lptr, const SharedPtr<Y>& rptr) {
	return lptr.isValid() || rptr.isValid();
}

template<typename X>
bool operator||(const SharedPtr<X>& lptr, bool rval) {
	return lptr.isValid() || rval;
}

template<typename X>
bool operator||(bool lval, const SharedPtr<X>& rptr) {
	return lval || rptr.isValid();
}

template<typename X>
bool operator!(const SharedPtr<X>& p) {
	return (!p.isValid());
}

/* less than comparisons for storage in containers */
template<typename X, typename Y>
bool operator<(const SharedPtr<X>& lptr, const SharedPtr<Y>& rptr) {
	return lptr.getRawPointer() < rptr.getRawPointer();
}

template<typename X, typename Y>
bool operator<(const SharedPtr<X>& lptr, Y* raw) {
	return lptr.getRawPointer() < raw;
}

template<typename X, typename Y>
bool operator<(X* raw, const SharedPtr<Y>& rptr) {
	return raw < rptr.getRawPointer();
}


#endif /* SEISBASE_SHARED_POINTER_H_ */
