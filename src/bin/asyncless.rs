use std::{
    future::Future,
    io::{Error, ErrorKind},
    pin::{pin, Pin},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    time::Sleep,
};

/// An uploader that simulates uploading bytes to a remote server.
///
/// A real implementation, with an actual remote uploader, could be used to
/// upload an incoming stream of bytes in an asynchronous manner.
#[pin_project::pin_project]
struct AsyncUploader {
    #[pin]
    fut: Option<UploadFuture>,
}

impl AsyncUploader {
    fn new() -> Self {
        Self { fut: None }
    }

    /// This is the slow upload function, which we want to use inside the
    /// AsyncWrite implementation.
    fn upload_bytes(&self, data: &[u8]) -> Result<UploadFuture, Error> {
        let content = String::from_utf8(data.to_vec())
            .map_err(|e| Error::new(ErrorKind::Other, format!("{e}")))?;
        Ok(UploadFuture {
            content,
            fut: tokio::time::sleep(tokio::time::Duration::from_secs(1)),
        })
    }
}

struct UploadFuture {
    content: String,
    fut: Sleep,
}

impl Future for UploadFuture {
    type Output = ();
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        // Make a copy of content before moving out of self.
        let content = self.content.clone();
        let pinned = unsafe { self.map_unchecked_mut(|s| &mut s.fut) };
        match pinned.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => {
                println!("Uploaded {}", content);
                Poll::Ready(())
            }
        }
    }
}

impl AsyncWrite for AsyncUploader {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        // Method `project` consumes `self`. Before calling it on self,
        // let's create the future that we might need later.
        let fut = self.upload_bytes(buf);

        let mut this = self.project();

        if this.fut.is_none() {
            this.fut.set(fut.ok());
            // The future we created is not necessarily completed yet, but
            // we successfully used all the input bytes, and are now ready
            // to accept new bytes. The poll_write contract requires us to
            // return Ready.
            return Poll::Ready(Ok(buf.len()));
        }

        // We have an on-going future. We can only upload the given bytes
        // in buf, and create a new Future, if the previous future is
        // complete. Otherwise, we return Pending or Ready with an error.
        match this.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => match fut {
                Ok(fut) => {
                    this.fut.set(Some(fut));
                    Poll::Ready(Ok(buf.len()))
                }
                Err(err) => Poll::Ready(Err(err)),
            },
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        // We don't have an inner buffer to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        if self.fut.is_none() {
            return Poll::Ready(Ok(()));
        }

        let mut this = self.project();

        // Same as poll_write, but we don't create a new Future if the
        // existing one is complete.
        match this.fut.as_mut().as_pin_mut().unwrap().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                // Clean up fut, just in case; even though it is
                // recommended that once shutdown is called the write
                // method is no longer called.
                this.fut.set(None);
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut writer = pin!(AsyncUploader::new());

    // In reality, we'd read from an input stream instead of this for loop!
    for i in 0..10 {
        let msg = format!("{i}: Hello, world!\n");
        writer.write_all(msg.as_bytes()).await.unwrap();
    }
    writer.shutdown().await.unwrap();
}
