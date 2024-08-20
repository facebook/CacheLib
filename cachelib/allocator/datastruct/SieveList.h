/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <map>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#pragma GCC diagnostic pop

#include "cachelib/common/CompilerUtils.h"
#include <atomic>

namespace facebook::cachelib {
// node information for the double linked list modelling the queue. 
// It has the previous, next, a visited bit information and 
// the last time the item was updated in Sieve.
template <typename T>
struct CACHELIB_PACKED_ATTR SieveListHook {
  using Time = uint32_t;
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;

  void setNext(T* const n, const PtrCompressor& compressor) noexcept {
    next_ = compressor.compress(n);
  }

  void setNext(CompressedPtr next) noexcept { next_ = next; }

  void setPrev(T* const p, const PtrCompressor& compressor) noexcept {
    prev_ = compressor.compress(p);
  }

  void setPrev(CompressedPtr prev) noexcept { prev_ = prev; }

  CompressedPtr getNext() const noexcept { return CompressedPtr(next_); }

  T* getNext(const PtrCompressor& compressor) const noexcept {
    return compressor.unCompress(next_);
  }

  CompressedPtr getPrev() const noexcept { return CompressedPtr(prev_); }

  T* getPrev(const PtrCompressor& compressor) const noexcept {
    return compressor.unCompress(prev_);
  }

  // set and get the time when the node was updated in the sieve.
  void setUpdateTime(Time time) noexcept { updateTime_ = time; }

  Time getUpdateTime() const noexcept {
    // Suppress TSAN here because we don't care if an item is promoted twice by
    // two get operations running concurrently. It should be very rarely and is
    // just a minor inefficiency if it happens.
    folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
    return updateTime_;
  }

 
 private:
  CompressedPtr next_{}; // next node in the linked list
  CompressedPtr prev_{}; // previous node in the linked list
  // timestamp when this was last updated to the head of the list
  Time updateTime_{0};
};

// uses a double linked list to implement an LRU. T must be have a public
// member of type Hook and HookPtr must point to that.
template <typename T, SieveListHook<T> T::*HookPtr>
class SieveList {
 public:
  using CompressedPtr = typename T::CompressedPtr;
  using PtrCompressor = typename T::PtrCompressor;
  using SieveListObject = serialization::SieveListObject;
  using RefFlags = typename T::Flags;
  using Mutex = folly::DistributedMutex;
  using LockHolder = std::unique_lock<Mutex>;


  SieveList() = default;
  SieveList(const SieveList&) = delete;
  SieveList& operator=(const SieveList&) = delete;

  explicit SieveList(PtrCompressor compressor) noexcept
      : compressor_(std::move(compressor)) {}

  // Restore SieveList from saved state.
  //
  // @param object              Save SieveList object
  // @param compressor          PtrCompressor object
  SieveList(const SieveListObject& object, PtrCompressor compressor)
      : compressor_(std::move(compressor)),
        head_(compressor_.unCompress(CompressedPtr{*object.compressedHead()})),
        tail_(compressor_.unCompress(CompressedPtr{*object.compressedTail()})),
	hand_(compressor_.unCompress(CompressedPtr{*object.compressedHand()})),
        size_(*object.size()) {}

  /**
   * Exports the current state as a thrift object for later restoration.
   */
  SieveListObject saveState() const {
    SieveListObject state;
    *state.compressedHead() = compressor_.compress(head_).saveState();
    *state.compressedTail() = compressor_.compress(tail_).saveState();
    *state.compressedHand() = compressor_.compress(hand_).saveState();
    *state.size() = size_;
    return state;
  }

  T* getNext(const T& node) const noexcept {
    return (node.*HookPtr).getNext(compressor_);
  }

  T* getPrev(const T& node) const noexcept {
    return (node.*HookPtr).getPrev(compressor_);
  }

  void setNext(T& node, T* next) noexcept {
    (node.*HookPtr).setNext(next, compressor_);
  }

  void setNextFrom(T& node, const T& other) noexcept {
    (node.*HookPtr).setNext((other.*HookPtr).getNext());
  }

  void setPrev(T& node, T* prev) noexcept {
    (node.*HookPtr).setPrev(prev, compressor_);
  }

  void setPrevFrom(T& node, const T& other) noexcept {
    (node.*HookPtr).setPrev((other.*HookPtr).getPrev());
  }
  
  // set the access bit of node
  void setAsVisited(T& node) noexcept;
  
  // unset the access bit of node
  void setAsUnvisited (T& node) noexcept;

  // return the access bit of node
  bool isVisited (const T& node) const noexcept;

  // return and update hand_ to point to the next item to be evicted  
  T* operateHand() noexcept;

  // Links the passed node to the head of the double linked list
  // @param node node to be linked at the head
  void linkAtHead(T& node) noexcept;

  
  // Links the passed node to the tail of the double linked list
  // @param node node to be linked at the tail
  void linkAtTail(T& node) noexcept; 
 
  // removes the node completely from the linked list and cleans up the node
  // appropriately by setting its next and prev as nullptr.
  void remove(T& node) noexcept;
  
  
  // Unlinks the destination node and replaces it with the source node
  //
  // @param oldNode   destination node
  // @param newNode   source node
  void replace(T& oldNode, T& newNode) noexcept;
  
  // moves a node that belongs to the linked list to the head of the linked
  // list.
  //void moveToHead(T& node) noexcept;
  
  T* getHead() const noexcept { return head_; }
  T* getTail() const noexcept { return tail_; }
  T* getHand() const noexcept {return hand_; }

  size_t size() const noexcept { return size_; }


  void inspectVisitMap() noexcept;

  void inspectSieveList() noexcept;


  // Iterator interface for the double linked list. Supports both iterating
  // from the tail and head.
  class Iterator {
   public:
    //enum class Direction { FROM_HEAD, FROM_TAIL };
    Iterator(T* p, SieveList<T, HookPtr>& sievelist) noexcept 
	    : curr_(p),sievelist_(&sievelist) {};

    //Iterator(T* p, Direction d, SieveList<T, HookPtr>& sievelist) noexcept
    //    : curr_(p), dir_(d), sievelist_(&sievelist) {}
    virtual ~Iterator() = default;

    // copyable and movable
    Iterator(const Iterator&) = default;
    Iterator& operator=(const Iterator&) = default;
    Iterator(Iterator&&) noexcept = default;
    Iterator& operator=(Iterator&&) noexcept = default;

    // moves the iterator forward and backward. Calling ++ once the iterator
    // has reached the end is undefined.
    Iterator& operator++() noexcept;
    Iterator& operator--() noexcept;

    T* operator->() const noexcept { return curr_; }
    T& operator*() const noexcept { return *curr_; }

    bool operator==(const Iterator& other) const noexcept {
      return sievelist_ == other.sievelist_ && curr_ == other.curr_ ;
    }

    bool operator!=(const Iterator& other) const noexcept {
      return !(*this == other);
    }

    explicit operator bool() const noexcept {
      return curr_ != nullptr && sievelist_ != nullptr;
    }

    T* get() const noexcept { 
	return curr_; 
    }
	
    // Invalidates this iterator
    void reset() noexcept { curr_ = nullptr; }
   
    // Reset the iterator back to the beginning
    void resetToBegin() noexcept { sievelist_->tail_;}

   protected:
    
    // the current position of the iterator in the list
    T* curr_{nullptr};
    // sievelist 
    SieveList<T, HookPtr>* sievelist_{nullptr};
  }; 

  // provides an iterator starting from a certain node to the head
  Iterator iterBackFromHand()  noexcept;

 private:
  // unlinks the node from the linked list. Does not correct the next and
  // previous.
  void unlink(const T& node) noexcept;

  const PtrCompressor compressor_{};

  //mutable folly::cacheline_aligned<Mutex> sievelistMutex_;

  // head of the linked list
  T* head_{nullptr};

  // tail of the linked list
  T* tail_{nullptr};

  // hand for eviction
  T* hand_{nullptr};

  // size of the list
  size_t size_{0};
};



/* Linked list implemenation */

template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::setAsVisited(T& node) noexcept{
    node.template setFlag<RefFlags::kMMFlag1>();
}

template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::setAsUnvisited(T& node) noexcept{
    node.template unSetFlag<RefFlags::kMMFlag1>();
}


template <typename T, SieveListHook<T> T::*HookPtr>
bool SieveList<T, HookPtr>::isVisited(const T& node) const noexcept{
   return node.template isFlagSet<RefFlags::kMMFlag1>();
}


template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::inspectSieveList() noexcept{
  std::cout << "Inspecting sieve_cache..."<<std::endl;
  T* curr = head_;
  T* prev = nullptr;
  T* next = nullptr;
  while (curr){
    prev = getPrev(*curr);
    next = getNext(*curr);
    /*
    std::cout << curr->getKey().toString() <<"," << curr;
    std::cout<<", visited: "<< isVisited(*curr);
    
    if (prev) std::cout<<". prev: "<< prev->getKey().toString() << ", " << prev;
    else std::cout << ". prev: null";
    if (next) std::cout<<", next: " << next->getKey().toString() << ", " << next;
    else std::cout << ". next:null"; 
    */
    
    std::cout << curr->getKey().toString() << ", visited: "<< isVisited(*curr);
    
    if (curr == hand_) std::cout << ". Hand" << std::endl;
    else std::cout<< std::endl;
    curr = getNext(*curr);
  }
  if (hand_ == nullptr) std::cout << "Hand is null" << std::endl;
  std::cout << "done inspecting." << std::endl;
}


template <typename T, SieveListHook<T> T::*HookPtr>
T* SieveList<T, HookPtr>::operateHand() noexcept{  
  T* curr = hand_;
  
  if (curr == nullptr) curr = tail_;
  if (curr == nullptr) return nullptr;
  while (isVisited(*curr)){
    setAsUnvisited(*curr);
    curr = getPrev(*curr);
    if (curr == nullptr) curr = tail_;
  }

  hand_ = getPrev(*curr); 
  return curr;
}


template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::linkAtHead(T& node) noexcept {
  XDCHECK_NE(reinterpret_cast<uintptr_t>(&node),
             reinterpret_cast<uintptr_t>(head_));
  
  setPrev(node, nullptr);
  setNext(node, head_); 
  
  // fix the prev ptr of head
  if (head_ != nullptr) {
    setPrev(*head_, &node);
  }
  head_ = &node;
  if (tail_ == nullptr) {
    tail_ = &node;
  }
  setAsUnvisited(node);
  size_++; 
}

template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::unlink(const T& node) noexcept {
   
  XDCHECK_GT(size_, 0u);
  // fix head_ and tail_ if the node is either of that.
  auto* const prev = getPrev(node);
  auto* const next = getNext(node); 

  if (&node == head_) {
    head_ = next;
  }

  if (&node == tail_) { 
    tail_ = prev;
  }

  if (&node == hand_){
    hand_ = prev;
  }
  // fix the next and prev ptrs of the node before and after us.
  if (prev != nullptr) {
    setNextFrom(*prev, node);
  }
  if (next != nullptr) {
    setPrevFrom(*next, node);
  }
  size_--;
}

template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::remove(T& node) noexcept {
  unlink(node);
  setNext(node, nullptr);
  setPrev(node, nullptr); 
}

template <typename T, SieveListHook<T> T::*HookPtr>
void SieveList<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  if (&oldNode == head_) {
    head_ = &newNode;
  }
  if (&oldNode == tail_) {
    tail_ = &newNode;
  } 
  if (&oldNode == hand_){
    hand_ = getPrev(*hand_);
  }
  
  setAsUnvisited(newNode);
  // Make the previous and next nodes point to the new node
  auto* const prev = getPrev(oldNode);
  auto* const next = getNext(oldNode);
  if (prev != nullptr) {
    setNext(*prev, &newNode);
  }
  if (next != nullptr) {
    setPrev(*next, &newNode);
  }

  // Make the new node point to the previous and next nodes
  setPrev(newNode, prev);
  setNext(newNode, next);

  // Cleanup the old node
  setPrev(oldNode, nullptr);
  setNext(oldNode, nullptr);
}


template <typename T, SieveListHook<T> T::*HookPtr>
typename SieveList<T, HookPtr>::Iterator&
SieveList<T, HookPtr>::Iterator::operator++() noexcept { 
  curr_ = sievelist_->operateHand(); 
  return *this;
}

template <typename T, SieveListHook<T> T::*HookPtr>
typename SieveList<T, HookPtr>::Iterator SieveList<T, HookPtr>::iterBackFromHand() noexcept{ 
  auto firstNodeToBeEvicted = operateHand();
  auto iterObj = SieveList<T, HookPtr>::Iterator(firstNodeToBeEvicted,*this);  
  return iterObj;
}
} // namespace facebook::cachelib
