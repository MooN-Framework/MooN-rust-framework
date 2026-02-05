use std::slice;
use std::alloc::{Layout, alloc, dealloc};
use crc32fast::Hasher;

pub struct MemAlloc {
    pub ptr: *mut u8,
    pub layout: Layout,
}

impl MemAlloc {
    pub fn new(size: usize) -> Option<Self> {
        let layout = Layout::from_size_align(size, 1)
            .expect("Failed to create Layout for memory allocation.");
        let ptr = unsafe { alloc(layout) };
        if !ptr.is_null() {
            return Some(Self { ptr, layout });
        }
        None
    }
    pub fn new_null() -> Option<Self> {
        let ptr = std::ptr::null_mut();
        let layout: Layout = Layout::from_size_align(1, 1).expect("Couldn't create 1 1 layout.");
        Some(Self { ptr, layout })
    }
    pub fn free(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                dealloc(self.ptr, self.layout);
                self.ptr = std::ptr::null_mut();
            }
        }
    }
    pub fn calculate_crc(&self) -> u32 {
        unsafe {
            let bytes: &[u8] = slice::from_raw_parts(self.ptr, self.layout.size());
            let mut hasher = Hasher::new();
            hasher.update(bytes);
            hasher.finalize()
        }
    }
    pub fn write(&self, data: &[u8]) -> Result<(), &'static str> {
        unsafe {
            if data.len() > self.layout.size() {
                return Err("Data too large to write into allocated memory.");
            }
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr, data.len());
        }
        Ok(())
    }
}
