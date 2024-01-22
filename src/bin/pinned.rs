use futures::future::BoxFuture;
use std::{
    io::{Error, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// An uploader that simulates uploading bytes to a remote server.
///
/// A real implementation, with an actual remote uploader, could be used to
/// upload an incoming stream of bytes in an asynchronous manner.
struct AsyncUploader {
    /// BoxFuture is a Boxed and Pinned future. The future wrapped in it
    /// does not have to be Unpin. This makes BoxFuture suitable for
    /// working with Futures created from calling an async function.
    fut: Option<BoxFuture<'static, Result<(), Error>>>,
}

impl AsyncUploader {
    fn new() -> Self {
        Self { fut: None }
    }
}

/// This is the slow upload function, which we want to use inside the
/// AsyncWrite implementation.
async fn upload_bytes(data: Vec<u8>) -> Result<(), Error> {
    // Sleep for a bit to simulate a long-running operation.
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    println!(
        "Uploaded {}",
        String::from_utf8(data)
            .map_err(|e| Error::new(ErrorKind::Other, format!("{e}")))?
    );
    Ok(())
}

impl AsyncWrite for AsyncUploader {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        if self.fut.is_none() {
            // We need a vec to avoid lifetime issues.
            let data = buf.to_vec();
            self.fut = Some(Box::pin(upload_bytes(data)));
            // The future we created is not necessarily completed yet, but
            // we successfully used all the input bytes, and are now ready
            // to accept new bytes. The poll_write contract requires us to
            // return Ready.
            return Poll::Ready(Ok(buf.len()));
        }

        // We have an on-going Future. We can only upload the given bytes
        // in buf, and create a new Future, if the previous one is
        // complete. Otherwise, we return Pending or Ready with an error.
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.fut = Some(Box::pin(upload_bytes(buf.to_vec())));
                Poll::Ready(Ok(buf.len()))
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
        // We don't have an inner buffer to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Error>> {
        if self.fut.is_none() {
            return Poll::Ready(Ok(()));
        }

        // Same as poll_write, but we don't create a new Future if the
        // existing one is complete.
        match self.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                // Clean up fut, just in case; even though it is
                // recommended that once shutdown is called the write
                // method is no longer called.
                let _ = self.fut.take();
                Poll::Ready(res)
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let mut writer = AsyncUploader::new();

    // In reality, we'd read from an input stream instead of this for loop!
    for i in 0..10 {
        let msg = format!("{i}: Hello, world!\n");
        writer.write_all(msg.as_bytes()).await.unwrap();
    }
    writer.shutdown().await.unwrap();
}
