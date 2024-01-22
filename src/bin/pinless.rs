use futures::{future::BoxFuture, Future};
use std::{
    io::{Error, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

struct UploadFuture {
    // Inner Future, resulted from an async call.
    fut: BoxFuture<'static, Result<(), Error>>,
}

impl Future for UploadFuture {
    type Output = Result<(), Error>;
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        match self.fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(o) => Poll::Ready(o),
        }
    }
}

/// An uploader that simulates uploading bytes to a remote server. This
/// uploader has an internal buffer. It collects the incoming bytes in its
/// buffer, and only uploads them when the buffer reaches a minimum size.
///
/// A real implementation, with an actual remote uploader, could be used to
/// upload an incoming stream of bytes in an asynchronous manner.
struct AsyncUploader {
    /// Minimum size of the chunks to upload.
    min_size: usize,
    /// Internal buffer.
    buf: Vec<u8>,
    /// Future corresponding to the long-running upload operation.
    fut: Option<UploadFuture>,
}

impl AsyncUploader {
    fn new(min_size: usize) -> Self {
        Self {
            min_size,
            // Initialize the inner vec with a simple heuristic for its
            // capacity.
            buf: Vec::with_capacity(2 * min_size),
            fut: None,
        }
    }

    /// Precondition: This method should only be called if self.fut is None
    /// or contains a completed future.
    fn upload(&mut self, buf: &[u8]) -> usize {
        // Here we could check that the stated precondition about self.fut
        // holds. Checking the completion of the Future in self.fut is only
        // possible via a call to poll, which requires passing to it a
        // Context object. Doing that requires changing the signature of
        // the method, and passing to it a Context object.

        let mut size = 0;
        if self.buf.len() < self.min_size {
            self.buf.extend_from_slice(buf);
            size = buf.len();
        }

        if self.buf.len() >= self.min_size {
            let data = self.buf.drain(..).collect();
            self.fut = Some(self.upload_bytes(data));
        } else {
            // It is safe to set self.fut to None because of the
            // precondition. Without setting fut to none, we'd continue
            // polling on it after completion, which results in a panic.
            self.fut = None;
        }

        size
    }

    /// This is the slow upload function, which we want to use inside the
    /// AsyncWrite implementation.
    fn upload_bytes(&mut self, data: Vec<u8>) -> UploadFuture {
        let fut = async {
            // Sleep for a bit to simulate a long-running operation.
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            println!(
                "Uploaded {}",
                String::from_utf8(data).map_err(|e| {
                    Error::new(ErrorKind::Other, format!("{e}"))
                })?
            );
            Ok(())
        };

        UploadFuture { fut: Box::pin(fut) }
    }

    /// Precondition: This method should only be called if self.fut is None
    /// or contains a completed future.
    fn upload_last(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        if self.buf.is_empty() {
            // We are done and must return Ready. Otherwise, we will call
            // poll on a Future which we already know is completed. That
            // would result in a "resumed after completion" panic.
            return Poll::Ready(Ok(()));
        } else {
            // Flush the inner buffer.
            let data = self.buf.drain(..).collect();
            self.fut = Some(self.upload_bytes(data));
        }

        self.fut
            .as_mut()
            .map(|f| Pin::new(f).poll(cx))
            .unwrap_or(Poll::Ready(Ok(())))
    }
}

impl AsyncWrite for AsyncUploader {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        if self.fut.is_none() {
            let size = self.upload(buf);
            // The future we created is not necessarily completed yet, but
            // we successfully used all the input bytes, and are now ready
            // to accept new bytes. The poll_write contract requires us to
            // return Ready.
            return Poll::Ready(Ok(size));
        }

        let pinned = Pin::new(self.fut.as_mut().unwrap());

        // We have an on-going future. We can only upload the given bytes
        // in buf, and create a new Future, if the previous future
        // is complete. Otherwise, we return Pending or an error.
        match pinned.poll(cx) {
            Poll::Pending => {
                // For simplicity, we don't use the incoming bytes if we
                // have an on-going future. For a more performant
                // implementation, one should add incoming bytes to the
                // inner buffer as long as its capacity allows, and only
                // return Pending if the buffer is full, and the Future in
                // fut is not yet complete.
                Poll::Pending
            }
            Poll::Ready(Ok(())) => {
                let size = self.upload(buf);
                Poll::Ready(Ok(size))
            }
            Poll::Ready(Err(err)) => {
                // Clean up fut to avoid a "resumed after completion"
                // panic, if for whatever reason the caller decides to
                // ignore this error and proceed with another write or a
                // call to shutdown.
                let _ = self.fut.take();
                Poll::Ready(Err(err))
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        // We don't flush the inner buffer, because with the current
        // implementation, the inner buffer size would not satisfy the
        // minimum size requirement. Therefore, flushing would violate the
        // invariant of the AsyncUploader.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        // Make sure any remaining bytes in the buffer are uploaded before
        // shutting down.
        match self.fut.as_mut() {
            None => self.upload_last(cx),
            Some(fut) => match fut.fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => self.upload_last(cx),
                Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            },
        }
    }
}

#[tokio::main]
async fn main() {
    let mut writer = AsyncUploader::new(128);

    // In reality, we'd read from an input stream instead of this for loop!
    for i in 0..10 {
        let msg = format!("{i}: Hello, world!\n");
        writer.write_all(msg.as_bytes()).await.unwrap();
    }
    writer.shutdown().await.unwrap();
}
