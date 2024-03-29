#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    bpm_ = std::exchange(that.bpm_, nullptr);
    page_ = std::exchange(that.page_, nullptr);
    is_dirty_ = std::exchange(that.is_dirty_, false);
}

void BasicPageGuard::Drop() {
 if (page_ && bpm_) {
        if (is_dirty_) {
            bpm_->FlushPage(page_->GetPageId());
        }
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        bpm_ = nullptr;
        page_ = nullptr;
        is_dirty_ = false;
    }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
 if (this != &that) { // 检查自赋值
        // 释放当前对象持有的资源
        Drop();

        // 将新对象的资源移动到当前对象
        bpm_ = std::exchange(that.bpm_, nullptr);
        page_ = std::exchange(that.page_, nullptr);
        is_dirty_ = std::exchange(that.is_dirty_, false);
    }
    return *this;
}

BasicPageGuard::~BasicPageGuard(){
   Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
 return ReadPageGuard(bpm_, page_);
//return {bpm_, page_}; 
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
return WritePageGuard(bpm_, page_);
//return {bpm_, page_}; 
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) :guard_(bpm, page){ guard_.page_->RLatch(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {   guard_.page_->RLatch(); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
   guard_ = std::move(that.guard_);

return *this; }

void ReadPageGuard::Drop() {
    guard_.page_->RUnlatch();
     guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
     Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page): guard_(bpm, page) {    guard_.page_->WLatch();  }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept :guard_(std::move(that.guard_)) {   guard_.page_->WLatch();}


auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
 if (this != &that) {
        guard_ = std::move(that.guard_);
    }
    return *this;
  }

void WritePageGuard::Drop() {
    guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace bustub
