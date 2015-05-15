/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2014 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/****************************************************************************/

#include <util/arena/LimitedArena.h>                     // For LimitedArena
#include <boost/array.hpp>                               // For boost::array
#include <bitset>                                        // For std::bitset
#include "ArenaDetails.h"                                // For implementation
#include "ArenaHeader.h"                                 // For Header

/****************************************************************************/
namespace scidb { namespace arena { namespace {
/****************************************************************************/

template<class,class>                                    // For each header
class Link;                                              // Adds list links
class Page;                                              // Large allocation
class Live;                                              // Small allocation
class Dead;                                              // Adds binning info

/****************************************************************************/

/**
 *  @brief      Adds intrusive linked list handling to class 'base'.
 *
 *  @details    Class Link<t,b>  adds a pair of pointers to the base class 'b'
 *              that allow it to maintain an intrusively linked list that runs
 *              through each of its instances.
 *
 *              It also implements a number of other member functions that are
 *              common to both the Page and Dead header classes, including:
 *
 *              - size()       returns the overall size of the allocation that
 *              is described by this header.
 *
 *              - length()     returns the size of  the payload portion of the
 *              allocation that is described by this header.
 *
 *              - payload()    returns a pointer to the payload portion of the
 *              allocation that is described by this header.
 *
 *              The template assumes that its base 'b' can be initialized with
 *              a size and that this lives in a member named '_size,  and also
 *              that the base provides a function named overhead() that yields
 *              the number of words occupied by the header itself.
 *
 *  @author     jbell@paradigm4.com.
 */
template<class type,class base>
class Link : public base, noncopyable
{
 protected:                // Construction
                              Link(size_t size)
                               : base(size),
                                _prev(0),
                                _next(0)                 {}

 public:                   // Header Operations
            size_t            size()               const {return base::_size;}
            size_t            length()             const {return size()-base::overhead();}
            bool              consistent()         const;// Verify consistency
            void*             payload();                 // Return the payload

 public:                   // List Operations
            bool              empty()              const {return _prev==0 && _next==0;}
            void              unlink()                   {_prev = _next = 0;}

 public:                   // List Operations
    static  void              push(type*&,type*);        // Insert onto list
    static  void              drop(type*&,type*);        // Remove from list
    static  type*             pop (type*&);              // Pop head of list

 private:                  // Representation
            type*             _prev;                     // Next item on list
            type*             _next;                     // Prev item on list
};

/**
 *  @brief      Implements the base class for class Page, defined below.
 *
 *  @author     jbell@paradigm4.com.
 */
class Page_
{
 public:                   // Construction
                              Page_(size_t size)         // Page size in words
                               : _size(size)             {assert(_size==size);}

 public:                   // Operations
    static  size_t            overhead()                 {return asWords(sizeof(Link<Page_,Page_>));}

 protected:                // Representation
            size_t      const _size;                     // Page size in words
};

/**
 *  @brief      Represents a large allocation from which many small blocks are
 *              then sub-allocated.
 *
 *  @details    Class Page provides the header for a large hunk of memory from
 *              which many smaller blocks are then sub-allocated. It includes:
 *
 *              - the overall size of the allocation, expressed in words.
 *
 *              - pointers to the previous and next pages in the arena's list
 *              of currently active pages.
 *
 *  @author     jbell@paradigm4.com.
 */
class Page : public Link<Page,Page_>
{
 public:                   // Operations
                              Page(size_t size)          // Page size in words
                               : Link<Page,Page_>(size)  {}
};

/**
 *  @brief      Represents a live allocation made within some page.
 *
 *  @details    Class Live provides the header for a live block of memory that
 *              resides within a page, and that is currently being used by the
 *              clients of the arena. It includes:
 *
 *              - a bit to indicate whether the block is live or dead; used to
 *              determine whether a dead neighbouring block can be merged with
 *              this block or not.
 *
 *              - a bit to indicate whether this block sits at the very end of
 *              the page or else is followed by a 'successor' block.
 *
 *              - the offset in words of the block's 'predecessor', that block
 *              within the same page thats sits immediately before this block,
 *              or zero if this block is at the very front of the page.
 *
 *              - the total size in words of the block: this includes the size
 *              of both the header and also its payload.
 *
 *              Together these fields allow the arena to work out in which bin
 *              to place a block when killing it off, whether the block can be
 *              merged with its neighbours, and if so, whether the entire page
 *              in which it sits can be freed or not.
 *
 *  @author     jbell@paradigm4.com.
 */
class Live
{
 public:                   // Construction
                              Live(size_t size)          // Live size in words
                               : _live(0),               // Not yet resurrected
                                 _succ(0),               // No successor block
                                 _pred(0),               // No predecessor block
                                 _size(size)             {assert(_size==size);}

 public:                   // Operations
            bool              live()               const {return _live;}
            Dead*             dead();                    // Check the downcast
            Dead*             kill();                    // Mark dead and cast
            void              predecessor(Dead*);        // Update predecessor
    static  size_t            overhead()                 {return asWords(sizeof(Live));}
    static  size_t            smallest()                 {return asWords(sizeof(Link<Live,Live>));}

 protected:                // Representation
    static  size_t      const _bits = std::numeric_limits<size_t>::digits/2-1;
            size_t            _live : 1;                 // Currently in use?
            size_t            _succ : 1;                 // Successor exists?
            size_t            _pred : _bits;             // Predecessor offset
            size_t            _size : _bits;             // Full size in words
};

/**
 *  @brief      Represents a dead allocation made within some page that is now
 *              binned and awaiting reuse.
 *
 *  @details    Class Dead extends class Live,from which it derives, by adding
 *              pointers to the previous and  next similarly sized dead blocks
 *              that sit awaiting reuse in the same bin. The bin is, in effect
 *              just a pointer to the first dead block on this list. It adds:
 *
 *              - successor()     returns the block immediately following this
 *              one in the same page, or null if this block sits at the end of
 *              the page.
 *
 *              - predecessor()   returns the block immediately preceding this
 *              one in the same page,  or zero if this block sits at the start
 *              of the page.
 *
 *              - split(s)        truncates the block to 's' words and returns
 *              the offcut if successful, or null if there's insufficient room
 *              within the block to create such an offcut.
 *
 *              - merge(s)        extends this block into the storage occupied
 *              by its immediate successor 's'.
 *
 *              - resurrect()     marks the block as now being in use and then
 *              returns its payload.
 *
 *              - reusable()      returns true if the block does not occupy an
 *              entire page, and thus is a suitable candidate for binning.
 *
 *  @author     jbell@paradigm4.com.
 */
class Dead : public Link<Dead,Live>
{
 public:                   // Construction
                              Dead(size_t size)
                               : Link<Dead,Live>(size)   {}

 public:                   // Operations
            bool              reusable()           const {return _succ||_pred;}
            Live*             successor();               // Next block in page
            Live*             predecessor();             // Prev block in page
            Dead*             split(size_t);             // Split from surplus
            void              merge(Dead*);              // Merge w/ successor
            void*             resurrect()                {_live = true;return payload();}
};

/****************************************************************************/

/**
 *  Adjust the pointer 's' by adding (or subtracting) 'words' multiples of the
 *  alignment word size and then casting to the type of object that we believe
 *  actually resides there.
 */
template<class target>
target* delta_cast(void* s,long words)
{
    assert(aligned(s));                                  // Validate alignment

    target* t = reinterpret_cast<target*>(reinterpret_cast<alignment_t*>(s) + words);

    assert(aligned(t));                                  // Validate alignment

    return t;                                            // Return the target
}

/**
 *  Adjust the given payload pointer by rewinding it back past the header, so
 *  as to retreive a pointer to the original allocation header.
 */
template<class type>
type* retrieve(void* payload)
{
    assert(aligned(payload));                            // Validate arguments

    return delta_cast<type>(payload,-type::overhead());  // Rewind past header
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
template<class t,class b>
bool Link<t,b>::consistent() const
{
    assert(aligned(this));                               // Validate alignment
    assert(asWords(sizeof(b)) <= size());                // Account for header

    return true;                                         // Appears to be good
}

/**
 *  Return a pointer to the region of storage that lies immediately beyond the
 *  end of this header object.
 */
template<class t,class b>
void* Link<t,b>::payload()
{
    return delta_cast<t>(this,b::overhead());            // Step past overhead
}

/**
 *  Place 'link' onto the front of the given doubly-linked list.
 */
template<class t,class b>
void Link<t,b>::push(t*& list,t* link)
{
    assert(link!=0 && link->empty());                    // Validate arguments

    if (list != 0)                                       // Is list non empty?
    {
        list->_prev = link;                              // ...aim at new head
        link->_next = list;                              // ...aim at old head
    }

    list = link;                                         // Place link on list
}

/**
 *  Remove 'link' from the given doubly-linked list.
 */
template<class t,class b>
void Link<t,b>::drop(t*& list,t* link)
{
    assert(list!=0 && link!=0);                          // Validate arguments

    if (t* n = link->_next)                              // Followed by a link?
    {
        n->_prev = link->_prev;                          // ...disconnect it
    }

    if (t* p = link->_prev)                              // Preceded by a link?
    {
        p->_next = link->_next;                          // ...disconnect it
    }
    else                                                 // No preceding link
    {
        assert(list == link);                            // ...it's the first

        list = link->_next;                              // ...so step past it
    }

    link->unlink();                                      // Reset the pointers
}

/**
 *  Remove and return the initial link from the given doubly-linked list.
 */
template<class t,class b>
t* Link<t,b>::pop(t*& list)
{
    assert(list != 0);                                   // Validate arguments

    t* head = list;                                      // Save the list head

    if (list = list->_next, list!=0)                     // Followed by a link?
    {
        list->_prev = 0;                                 // ...disconnect it
    }

    head->unlink();                                      // Reset the pointers

    return head;                                         // Return the old head
}

/**
 *  Test the '_live' bit before safely downcasting to a Dead block header.
 */
Dead* Live::dead()
{
    return _live ? 0 : static_cast<Dead*>(this);         // Check the downcast
}

/**
 *  Mark the block as being no longer in use and cast it to back to the larger
 *  Dead block header that we know is really there; this is why we clamped the
 *  size of the original allocation  in doMalloc() to be at least sizeof(Dead)
 *  bytes long, but also represents a small source of waste: this arena can't,
 *  in fact, allocate a block smaller than sizeof(Dead) bytes.
 */
Dead* Live::kill()
{
    assert(_live);                                       // Validate our state

    _live = false;                                       // Mark block as dead

    Dead* d = static_cast<Dead*>(this);                  // Dowcast to 'Dead*'

    d->unlink();                                         // Reset bin pointers

    assert(d->consistent());                             // Check consistency
    return d;                                            // Return the header
}

/**
 *  Update the field '_pred' to point at the given preceding dead block, which
 *  has just been split into two.
 */
void Live::predecessor(Dead* dead)
{
    assert(this  == dead->successor());                  // Dead precedes this
    assert(_pred >= dead->size());                       // Dead has been split

    this->_pred = dead->size();                          // Update its offset
}

/**
 *  Return the block that immediately follows this one in the page, or null if
 *  this block sits at the very end of the page.
 */
Live* Dead::successor()
{
    if (_succ)                                           // Have a successor?
    {
        return delta_cast<Live>(this,_size);             // ...then return it
    }

    return 0;                                            // Has no successor
}

/**
 *  Return the block that this one immediately follows in the page, or null if
 *  this block sits at the very beginning of the page.
 */
Live* Dead::predecessor()
{
    if (_pred)                                           // Has a predecessor?
    {
        return delta_cast<Live>(this,-_pred);            // ...then return it
    }

    return 0;                                            // Has no predecessor
}

/**
 *  Split the block into two: the original, which is truncated to hold exactly
 *  'size' words, and an offcut - its 'successor' - that resides at the end of
 *  the original block and occupies all of the remaining space.
 *
 *  Return the offcut if there is sufficient space available to create one, or
 *  null if there is not.
 */
Dead* Dead::split(size_t size)
{
    assert(this->dead() && size>=Dead::smallest());      // Validate arguments

 /* Would truncating this block to carry a payload of exactly 'size' words
    leave sufficient room for at least the header of a subsequent block?...*/

    if (_size >= size + Dead::smallest())                // Room for an offcut?
    {
        void* v = delta_cast<void>(this,size);           // ...get the new end
        Dead* b = new(v) Dead(_size - size);             // ...carve an offcut

        b->_succ = _succ;                                // ...adopt successor
        b->_pred = size;                                 // ...we are its pred

        _size = size;                                    // ...shrink original
        _succ = true;                                    // ...has a successor

        if (Live* s = b->successor())                    // Has b a successor?
        {
            s->predecessor(b);                           // ...update its pred
        }

        assert(this->consistent() && b->consistent());   // ...check all is ok
        return b;                                        // ...return offcut b
    }

    return 0;                                            // No room for offcut
}

/**
 *  Grow this block into the space currently occupied by the block immediately
 *  following us within the same page.
 */
void Dead::merge(Dead* block)
{
    assert(this->dead() && block->dead());               // Must both be dead
    assert(block == this->successor());                  // Must be successor

    _size += block->_size;                               // Swallow the  block
    _succ  = block->_succ;                               // Only if he has one

    if (Live* s = block->successor())                    // Has successor too?
    {
        static_cast<Dead*>(s)->_pred = _size;            // ...set predecessor
    }

    assert(this->consistent() && block->consistent());   // Check both look ok
}

/****************************************************************************/
}
/****************************************************************************/

/**
 *  @brief      Adapts Doug Lea's memory allocator to implement an %arena that
 *              supports both recycling and resetting.
 *
 *  @details    Class LeaArena implements a hybrid %arena that allocates large
 *              pages of memory from its parent %arena from which it then sub-
 *              allocates the actual requests for memory, similarly to the way
 *              in which class ScopedArena works, but also accepts requests to
 *              eagerly recycle allocations,  which it handles by placing them
 *              on one of several intrusively linked lists known as 'bins'. In
 *              a sense the class implements a sort 'heap within a heap'.
 *
 *              The design is loosely based upon that of Doug Lea's well known
 *              allocator implementation. In particular, our binning strategy-
 *              the number and sizes of the bins - is adpated from his design,
 *              although we've not yet implemented all of the other heuristics
 *              found in his design, such as using trees to maintain the large
 *              bins in sorted order, nor the use of a 'designated victim' to
 *              optimize the locality of consecutive allocations.
 *
 *  @see        http://g.oswego.edu/dl/html/malloc.html for an overview of the
 *              design from which this implementation derives.
 *
 *  @author     jbell@paradigm4.com.
 */
class LeaArena : public LimitedArena
{
 public:                   // Construction
                              LeaArena(const Options&);
    virtual                  ~LeaArena();

 public:                   // Attributes
    virtual features_t        features()           const;
    virtual void              insert(std::ostream&)const;

 public:                   // Operations
    virtual void              reset();

 public:                   // Implementation
    virtual void*             doMalloc(size_t);
    virtual void              doFree  (void*,size_t);

 private:                  // Implementation
            Dead*             makePage(size_t);
            void              freePage(Page*);
            Dead*             reuse   (size_t&);
            void              merge   (Dead*&);
            void              unbin   (Dead*);
            void              rebin   (Dead*);
    static  size_t            bin     (const Dead*);
    static  size_t            bin     (size_t);

 private:                  // Implementation
            bool              consistent()        const;

 private:                  // Representation
     boost::array<Dead*,128> _bins;                      // The bin array
       std::bitset<     128> _used;                      // The bin usage map
     static size_t     const _size[128];                 // The bin sizes
            size_t     const _pgsz;                      // The page size
            Page*            _page;                      // The page list
};

/**
 *  Construct a resetting %arena that allocates storage o.pagesize() bytes at
 *  a time from the %arena o.%parent().
 */
    LeaArena::LeaArena(const Options& o)
            : LimitedArena(o),
              _pgsz(asWords(o.pagesize())),
              _page(0)
{
    _bins.fill(0);                                       // Clear out the bins
}

/**
 *  Reset the %arena, thus returning any remaining pages to our parent %arena.
 */
    LeaArena::~LeaArena()
{
    this->reset();                                       // Free all our pages
}

/**
 *  Return a bitfield indicating the set of features this %arena supports.
 */
features_t LeaArena::features() const
{
    return finalizing | resetting | recycling;           // Supported features
}

/**
 *  Overrides @a Arena::insert() to emit a few interesting members of our own.
 */
void LeaArena::insert(std::ostream& o) const
{
    LimitedArena::insert(o);                             // First insert base

    o <<",pagesize=" << words_t(_pgsz);                  // Emit the page size
}

/**
 *  Reset the %arena to its originally constructed state,destroying any extant
 *  objects, recycling their underlying storage for use in future allocations,
 *  and resetting the allocation statistics to their default values.
 */
void LeaArena::reset()
{
    _bins.fill(0);                                       // Clear out the bins
    _used.reset();                                       // Sync the usage map

    while (_page != 0)                                   // While pages remain
    {
        freePage(Page::pop(_page));                      // ...free first page
    }

    LimitedArena::reset();                               // Reset statistics

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate 'size' bytes of raw storage.
 *
 *  'size' may not be zero.
 *
 *  The result is correctly aligned to hold one or more 'alignment_t's.
 *
 *  The resulting allocation must eventually be returned to the same %arena by
 *  calling doFree(), and with the same value for 'size'.
 */
void* LeaArena::doMalloc(size_t size)
{
    assert(size != 0);                                   // Validate arguments

    size = asWords(size);                                // Convert into words
    size+= Live::overhead();                             // Account for header

    Dead* b = reuse(size);                               // Pop first suitable

    if (b == 0)                                          // No suitable block?
    {
        b = makePage(size);                              // ...make a huge one
    }

    if (Dead* r = b->split(size))                        // Larger than needed?
    {
        rebin(r);                                        // ...bin the offcut
    }

    assert(size<=b->size());                             // Request satisfied
    assert(consistent());                                // Check consistency

    return b->resurrect();                               // Mark block as live
}

/**
 *  Free the memory that was allocated earlier from the same %arena by calling
 *  malloc() and attempt to recycle it for future reuse to reclaim up to 'size'
 *  bytes of raw storage.  No promise is made as to *when* this memory will be
 *  made available again however: the %arena may, for example, prefer to defer
 *  recycling until a subsequent call to reset() is made.
 */
void LeaArena::doFree(void* payload,size_t)
{
    assert(aligned(payload));                            // Validate arguments

    Dead* b = retrieve<Live>(payload)->kill();           // Rewind past header

    merge(b);                                            // Merge w neighbours

    if (b->reusable())                                   // Has live neighbor?
    {
        rebin(b);                                        // ...rebin the block
    }
    else                                                 // Fills a whole page
    {
        Page* p = retrieve<Page>(b);                     // ...fetch the page

        Page::drop(_page,p);                             // ...drop from list

        freePage(p);                                     // ...release memory
    }

    assert(consistent());                                // Check consistency
}

/**
 *  Allocate a new large page from the parent arena and return its contents as
 *  a single dead block that is allocated within it.
 */
Dead* LeaArena::makePage(size_t size)
{
    assert(size >= Dead::smallest());                    // Validate argument

 /* Round up the page size to the nearest multiple of the requested size.
    This strategy tends to favour subsequent allocations of the same size,
    but may need a little tuning to avoid excessive waste... */

    if (size_t r = _pgsz % size)                         // Gives a remainder?
    {
        size += _pgsz - r;                               // ...round _pgsz up
    }
    else                                                 // Divides pages size
    if (size < _pgsz)                                    // But is it smaller?
    {
        size  = _pgsz;                                   // ...use _pgsz as is
    }

    size_t n = Page::overhead() + size;                  // Add page overhead
    void*  v = LimitedArena::doMalloc(asBytes(n));       // Allocate the page
    Page*  p = new(v) Page(n);                           // Create the header

    Page::push(_page,p);                                 // Push onto the list

 /* Construct a block within 'p' that occupies the entire payload area...*/

    return new(p->payload()) Dead(size);                 // Return page block
}

/**
 *  Return the allocation described the given page to our parent arena.
 */
void LeaArena::freePage(Page* page)
{
    assert(aligned(page));                               // Validate argument

    LimitedArena::doFree(page,asBytes(page->size()));    // Return to parent
}

/**
 *  Check the bins for the first available block that is big enough to satisfy
 *  a requested allocation size of 'size' words and, if found, pop it from the
 *  bin and update 'size' to reflect the actual size of the block we intend to
 *  allocate.
 *
 *  Throughout the code we have been keeping a bit vector of the bins that are
 *  in use for just this purpose: a scan of the map now quickly tells us which
 *  is the next largest bin to pop.
 */
Dead* LeaArena::reuse(size_t& size)
{
    size_t i = bin(size);                                // Find the bin index

    if (i >= _bins.size())                               // Too big for a bin?
    {
        return 0;                                        // ...make a new page
    }

    size = _size[i];                                     // Actual size wanted

    i = _used._Find_next(i-1);                           // Next non empty bin

    if (i >= _bins.size())                               // Failed to find one?
    {
        return 0;                                        // ...make a new page
    }

    Dead* d = Dead::pop(_bins[i]);                       // Pop the first dead

    _used[i] = _bins[i];                                 // Sync the usage map

    assert(d->size() >= size);                           // Confirm big enough
    return d;                                            // So we can reuse it
}

/**
 *  Attempt to merge the given block with those immediate neighbours allocated
 *  within the same page.
 */
void LeaArena::merge(Dead*& block)
{
    assert(aligned(block) && block->dead());             // Validate argument

    if (Live* s = block->successor())                    // Has a successor?
    {
        if (Dead* d = s->dead())                         // ...and it's dead?
        {
            unbin(d);                                    // ....pull from bin
            block->merge(d);                             // ....fuse together
        }
    }

    if (Live* p = block->predecessor())                  // Has a predecessor?
    {
        if (Dead* d = p->dead())                         // ...and it's dead?
        {
            unbin(d);                                    // ....pull from bin
            d->merge(block);                             // ....fuse together
            block = d;                                   // ....the new block
        }
    }
}

/**
 *  Remove the given block from whichever bin it is currently sitting in.
 */
void LeaArena::unbin(Dead* block)
{
    assert(aligned(block) && block->dead());             // Validate argument

    size_t i = bin(block);                               // Calculate its bin

    Dead::drop(_bins[i],block);                          // Drop from the bin

    _used[i] = _bins[i];                                 // Sync the usage map
}

/**
 *  Return the given block to whichever bin it belongs.
 */
void LeaArena::rebin(Dead* block)
{
    assert(aligned(block) && block->dead());             // Validate argument
    assert(block->reusable());                           // Do not bin a page

    size_t i = bin(block);                               // Calculate its bin

    Dead::push(_bins[i],block);                          // Toss into the bin

    _used[i] = true;                                     // Sync the usage map
}

/**
 *  Return the index of the bin in which the dead block 'd' should be placed.
 *
 *  That is, return the greatest 'i' such that d->size() >= _size[i].
 */
size_t LeaArena::bin(const Dead* d)
{
    return std::upper_bound(_size,_size+SCIDB_SIZE(_size),d->size())-_size-1;
}

/**
 *  Return the index of the first bin that is guarenteed to hold blocks all of
 *  which are at least 'size' words or larger.
 *
 *  That is, return the least 'i' such that size <= _size[i].
 */
size_t LeaArena::bin(size_t size)
{
    return std::lower_bound(_size,_size+SCIDB_SIZE(_size),size) - _size;
}

/**
 *  Return true if the object looks to be in good shape.  Centralizes a number
 *  of consistency checks that would otherwise clutter up the code, and, since
 *  only ever called from within assertions, can be eliminated entirely by the
 *  compiler from the release build.
 */
bool LeaArena::consistent() const
{
    assert(LimitedArena::consistent());                  // Check base is good

    for (size_t i=0,e=_bins.size(); i!=e; ++i)           // For every bin ...
    {
        assert(iff(_used[i],_bins[i]!=0));               // ...map is in sync

     /* Check that if the i'th bin has a block in it, then this block is at
        least as big as the bin size and that it is properly binned... */

        if (Dead* d = _bins[i])                          // ...bin has a block?
        {
            assert(d->size() >= _size[i]);               // ....is big enough
            assert(bin(d)    == i);                      // ....in proper bin
            static_cast<void>(d);                        // ....release build
        }
    }

    return true;                                         // Appears to be good
}

/**
 *  The table of bin sizes.
 *
 *  Half our bins hold blocks whose sizes match the bin size exactly. The rest
 *  of the bins handle dead blocks whose sizes fall within a range,  the upper
 *  bound of each range being spaced roughly logarithmically. There is nothing
 *  sacred about these values, however, and one can always tune the entries by
 *  using performance data taken from a run of the system in order to optimize
 *  the distribution of blocks amongst the bins.
 */
const size_t LeaArena::_size[] =
{
    asWords(       24),  //    0
    asWords(       32),  //    1
    asWords(       40),  //    2
    asWords(       48),  //    3
    asWords(       56),  //    4
    asWords(       64),  //    5
    asWords(       72),  //    6
    asWords(       80),  //    7
    asWords(       88),  //    8
    asWords(       96),  //    9
    asWords(      104),  //   10
    asWords(      112),  //   11
    asWords(      120),  //   12
    asWords(      128),  //   13
    asWords(      136),  //   14
    asWords(      144),  //   15
    asWords(      152),  //   16
    asWords(      160),  //   17
    asWords(      168),  //   18
    asWords(      176),  //   19
    asWords(      184),  //   20
    asWords(      192),  //   21
    asWords(      200),  //   22
    asWords(      208),  //   23
    asWords(      216),  //   24
    asWords(      224),  //   25
    asWords(      232),  //   26
    asWords(      240),  //   27
    asWords(      248),  //   28
    asWords(      256),  //   29
    asWords(      264),  //   30
    asWords(      272),  //   31
    asWords(      280),  //   32
    asWords(      288),  //   33
    asWords(      296),  //   34
    asWords(      304),  //   35
    asWords(      312),  //   36
    asWords(      320),  //   27
    asWords(      328),  //   38
    asWords(      336),  //   39
    asWords(      344),  //   40
    asWords(      352),  //   41
    asWords(      360),  //   42
    asWords(      368),  //   43
    asWords(      376),  //   44
    asWords(      384),  //   45
    asWords(      392),  //   46
    asWords(      400),  //   47
    asWords(      408),  //   48
    asWords(      416),  //   49
    asWords(      424),  //   50
    asWords(      432),  //   51
    asWords(      440),  //   52
    asWords(      448),  //   53
    asWords(      456),  //   54
    asWords(      464),  //   55
    asWords(      472),  //   56
    asWords(      480),  //   57
    asWords(      488),  //   58
    asWords(      496),  //   59
    asWords(      504),  //   60
    asWords(      512),  //   61
    asWords(      520),  //   62
    asWords(      528),  //   63
    asWords(      576),  //   64
    asWords(      640),  //   65
    asWords(      704),  //   66
    asWords(      768),  //   67
    asWords(      832),  //   68
    asWords(      896),  //   69
    asWords(      960),  //   70
    asWords(     1024),  //   71   2^10      1KiB
    asWords(     1088),  //   72
    asWords(     1152),  //   73
    asWords(     1216),  //   74
    asWords(     1280),  //   75
    asWords(     1344),  //   76
    asWords(     1408),  //   77
    asWords(     1472),  //   78
    asWords(     1536),  //   79
    asWords(     1600),  //   80
    asWords(     1664),  //   81
    asWords(     1728),  //   82
    asWords(     1792),  //   83
    asWords(     1856),  //   84
    asWords(     1920),  //   85
    asWords(     1984),  //   96
    asWords(     2048),  //   87   2^11     2KiB
    asWords(     2112),  //   88
    asWords(     2560),  //   89
    asWords(     3072),  //   90
    asWords(     3584),  //   91
    asWords(     4096),  //   92   2^12     4KiB
    asWords(     4608),  //   93
    asWords(     5120),  //   94            5KiB
    asWords(     5632),  //   95
    asWords(     6144),  //   96            6KiB
    asWords(     6656),  //   97
    asWords(     7168),  //   98            7KiB
    asWords(     7680),  //   99
    asWords(     8192),  //  100   2^13     8KiB
    asWords(     8704),  //  101
    asWords(     9216),  //  102            9KiB
    asWords(     9728),  //  103
    asWords(    10240),  //  104           10KiB
    asWords(    10752),  //  105
    asWords(    12288),  //  106           12KiB
    asWords(    16384),  //  107   2^14    16KiB
    asWords(    20480),  //  108           20KiB
    asWords(    24576),  //  109           24KiB
    asWords(    28672),  //  110           28KiB
    asWords(    32768),  //  111   2^15    32KiB
    asWords(    36864),  //  112           34KiB
    asWords(    40960),  //  113           40KiB
    asWords(    65536),  //  114   2^16    64KiB
    asWords(    98304),  //  115           96KiB
    asWords(   131072),  //  116   2^17   128KiB
    asWords(   163840),  //  117          160KiB
    asWords(   262144),  //  118   2^18   256KiB
    asWords(   524288),  //  119   2^19   512KiB
    asWords(  1048576),  //  120   2^20     1MiB
    asWords(  2097152),  //  121   2^21     2MiB
    asWords(  4194304),  //  122   2^22     4MiB
    asWords(  8388608),  //  123   2^23     8MiB
    asWords( 16777216),  //  124   2^24    16MiB
    asWords( 33554432),  //  125   2^25    32MiB
    asWords( 67108864),  //  126   2^26    64MiB
    asWords(134217728),  //  127   2^27   128MiB
};

/**
 *  Create and return a resetting %arena that constrains the %arena o.%parent()
 *  to allocating at most o.limit() bytes of memory before throwing an arena::
 *  Exhausted exception,  that allocates memory in pages of o.pagesize() bytes
 *  at a time, and that also supports eager recycling of memory alocations.
 */
ArenaPtr newLeaArena(const Options& o)
{
    return boost::make_shared<LeaArena>(o);              // Allocate new arena
}

/****************************************************************************/
}}
/****************************************************************************/
