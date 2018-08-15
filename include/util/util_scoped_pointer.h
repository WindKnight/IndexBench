/*
 * gcache_scoped_pointer.h
 *
 *  Created on: Jan 21, 2016
 *      Author: zch
 */

#ifndef GCACHE_SCOPED_POINTER_H_
#define GCACHE_SCOPED_POINTER_H_

#include <stdlib.h>



#define DISABLE_COPY(CLASS)  \
		CLASS(const CLASS&); \
		CLASS &operator=(const CLASS&);

template <typename T>
struct ScopedPointerDeleter
{
    static inline void cleanup(T *pointer)
    {
        // Enforce a complete type.
        // If you get a compile error here, read the secion on forward declared
        // classes in the ScopedPointer documentation.
        typedef char IsIncompleteType[ sizeof(T) ? 1 : -1 ];
        (void) sizeof(IsIncompleteType);

        delete pointer;
    }
};

template <typename T>
struct ScopedPointerArrayDeleter
{
    static inline void cleanup(T *pointer)
    {
        // Enforce a complete type.
        // If you get a compile error here, read the secion on forward declared
        // classes in the ScopedPointer documentation.
        typedef char IsIncompleteType[ sizeof(T) ? 1 : -1 ];
        (void) sizeof(IsIncompleteType);

        delete [] pointer;
    }
};

struct ScopedPointerPodDeleter
{
    static inline void cleanup(void *pointer) { if (pointer) free(pointer); }
};

template <typename T, typename Cleanup = ScopedPointerDeleter<T> >
class ScopedPointer
{

    typedef T *ScopedPointer:: *RestrictedBool;

public:
    explicit inline ScopedPointer(T *p = 0) : d(p)
    {
            _autoDel = true;
    }

    inline ~ScopedPointer()
    {
        T *oldD = this->d;
        if(_autoDel)
                Cleanup::cleanup(oldD);
        this->d = 0;
    }

    inline T &operator*() const
    {
        //Q_ASSERT(d);
        return *d;
    }

    inline T *operator->() const
    {
        //Q_ASSERT(d);
        return d;
    }

    inline bool operator!() const
    {
        return !d;
    }
/*
#if defined(Q_CC_NOKIAX86) || defined(Q_QDOC)
    inline operator bool() const
    {
        return isNull() ? 0 : &ScopedPointer::d;
    }
#else*/
    inline operator RestrictedBool() const
    {
        return isNull() ? 0 : &ScopedPointer::d;
    }
//#endif

    inline T *data() const
    {
        return d;
    }

    inline bool isNull() const
    {
        return !d;
    }

    inline void release()
    {
            _autoDel = false;
    }

    inline void reset(T *other = 0)
    {
        if (d == other)
            return;
        T *oldD = d;
        d = other;
        if(_autoDel)
                Cleanup::cleanup(oldD);

        _autoDel = true;
    }

    inline T *take()
    {
        T *oldD = d;
        d = 0;
        return oldD;
    }

    inline void swap(ScopedPointer<T, Cleanup> &other)
    {
        GCacheSwap(d, other.d);
    }

    typedef T *pointer;

protected:
    T *d;

private:
    DISABLE_COPY(ScopedPointer)
    bool _autoDel;
};

template <class T, class Cleanup>
inline bool operator==(const ScopedPointer<T, Cleanup> &lhs, const ScopedPointer<T, Cleanup> &rhs)
{
    return lhs.data() == rhs.data();
}

template <class T, class Cleanup>
inline bool operator!=(const ScopedPointer<T, Cleanup> &lhs, const ScopedPointer<T, Cleanup> &rhs)
{
    return lhs.data() != rhs.data();
}

template <class T, class Cleanup>
inline void GCacheSwap(ScopedPointer<T, Cleanup> &p1, ScopedPointer<T, Cleanup> &p2)
{ p1.swap(p2); }

template <typename T, typename Cleanup = ScopedPointerArrayDeleter<T> >
class ScopedArrayPointer : public ScopedPointer<T, Cleanup>
{
public:
    explicit inline ScopedArrayPointer(T *p = 0)
        : ScopedPointer<T, Cleanup>(p)
    {
    }

    inline T &operator[](int i)
    {
        return this->d[i];
    }

    inline const T &operator[](int i) const
    {
        return this->d[i];
    }

private:
    DISABLE_COPY(ScopedArrayPointer)
};




#endif /* GCACHE_SCOPED_POINTER_H_ */
