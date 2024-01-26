//! A simplified example of an async uploader.
//! For alternative implementations see the code in src/bin.

#![feature(try_blocks)]

use futures::future::BoxFuture;
use std::{
    io::{Error, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    fs::File,
    io::{AsyncWrite, AsyncWriteExt},
    task::JoinHandle,
};

/// An async file uploader that can be used to store a file in chunks.
/// These chunks can then be pieced together to reconstruct the entire
/// file.
/// Each chunk is uploaded as a byte vector. To ensure that all chunks are
/// completed successfully, the method [`complete`] must be called.
/// If uploading a chunk fails, there is no way to recover from it, in this
/// implementation!!
struct FileUploader {
    dest_dir: String,
    parts: Vec<JoinHandle<Result<(), Error>>>,
}

impl FileUploader {
    fn new(dest_dir: String) -> Self {
        Self {
            dest_dir,
            parts: Vec::new(),
        }
    }

    /// Write the given bytes to a new file identified by the given chunk
    /// number.
    fn upload_bytes(&mut self, bytes: Vec<u8>, chunk_num: u32) {
        let dest = self.dest_dir.clone();
        let handle = tokio::spawn(async move {
            let result: Result<(), Error> = try {
                let mut file =
                    File::create(format!("{dest}/chunk-{chunk_num}"))
                        .await?;
                file.write_all(&bytes).await?;
            };
            println!(
                "Uploading chunk {chunk_num} of size {} completed.",
                bytes.len()
            );
            result
        });
        self.parts.push(handle);
    }

    /// Wait for all upload tasks to complete.
    async fn complete(self) -> Result<(), Error> {
        for handle in self.parts {
            let _ = handle.await?;
        }

        Ok(())
    }
}

/// An AsyncUploader that maintains an internal buffer, and only uploads
/// the buffer content when a minimum size is reached.
///
/// This struct implements [`AsyncWrite`].
struct AsyncUploader {
    /// The inner uploader.
    uploader: Option<FileUploader>,
    /// Future to keep track of the completion of tasks.
    fut: Option<BoxFuture<'static, Result<(), Error>>>,
    /// The next chunk number.
    chunks: u32,
    /// Minimum size of the chunks to upload.
    min_size: usize,
    /// Internal buffer.
    buf: Vec<u8>,
}

impl AsyncUploader {
    fn new(dest_dir: &str, min_size: usize) -> Self {
        Self {
            fut: None,
            uploader: Some(FileUploader::new(dest_dir.to_string())),
            chunks: 0,
            min_size,
            // Initialize the inner vec with twice the min_size.
            buf: Vec::with_capacity(2 * min_size),
        }
    }

    /// Add the bytes to the inner buffer, and upload the buffer content if
    /// the [`min_size`] mark is reached.
    fn upload(&mut self, buf: &[u8]) -> usize {
        let mut size = 0;
        if self.buf.len() < self.min_size {
            self.buf.extend_from_slice(buf);
            size = buf.len();
        }

        if self.buf.len() >= self.min_size {
            println!("Uploading bytes!");
            let data = self.buf.drain(..).collect();
            self.upload_bytes(data);
        }

        size
    }

    /// Call the inner uploader, and increase the chunk number.
    fn upload_bytes(&mut self, data: Vec<u8>) {
        self.uploader
            .as_mut()
            .unwrap()
            .upload_bytes(data, self.chunks);
        self.chunks += 1;
    }
}

impl AsyncWrite for AsyncUploader {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        // In this simplified example, we don't put any limit on the
        // number of async tasks that the inner uploader creates. As a
        // result, we don't have a [`Future`] to keep track of. In a more
        // realistic scenario, this will not be a reasonable strategy. We
        // might need to keep track of [`Future`] and poll on it. Look at
        // [`src/bin`] for ways to implement that.
        let size = self.upload(buf);
        if size == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(size))
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
        // Call [`FileUploader::complete`] on the inner uploader, and
        // create a [`Future`] to keep track of the progress of
        // [`FileUploader::complete`].
        if self.fut.is_none() {
            if self.uploader.is_some() {
                // Upload the last chunk
                let data = self.buf.clone();
                self.upload_bytes(data);
                self.fut = Some(Box::pin(
                    self.uploader.take().unwrap().complete(),
                ));
            } else {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::Other,
                    "use after shutdown",
                )));
            }
        }

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

#[cfg(test)]
mod tests {
    use std::io::Error;

    use async_compression::tokio::write::{ZstdDecoder, ZstdEncoder};
    use chrono::Utc;
    use tokio::io::AsyncWriteExt;

    use crate::AsyncUploader;
    use base64::prelude::{Engine as _, BASE64_STANDARD};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct Message {
        message: String,
        base64: String,
    }

    /// Combine all chunks together.
    async fn combine_chunks(
        dir: &str,
        count: u32,
    ) -> Result<Vec<u8>, Error> {
        let mut all_bytes = vec![];
        for i in 0..count {
            let contents =
                tokio::fs::read(format!("{dir}/chunk-{i}")).await?;
            all_bytes.extend_from_slice(&contents);
        }

        Ok(all_bytes)
    }

    async fn generate_and_compress_data(
        dest: &str,
        count: usize,
    ) -> Result<Vec<u8>, Error> {
        std::fs::create_dir(dest)?;
        let mut async_uploader = AsyncUploader::new(dest, 1024);
        let mut encoder = ZstdEncoder::new(async_uploader);
        // Create and array of json objects.
        encoder.write_all(b"[").await?;
        for i in 0..count {
            // Generate data with some randomness, so that the compressed
            // content is not too small!
            let message = format!("Message {i} at  {}", Utc::now());
            let data = Message {
                base64: BASE64_STANDARD.encode(message.clone()),
                message,
            };
            encoder
                .write_all(serde_json::to_string(&data)?.as_bytes())
                .await?;
            if i + 1 < count {
                encoder.write_all(b",").await?;
            } else {
                encoder.write_all(b"]").await?;
            }
        }

        encoder.shutdown().await?;
        async_uploader = encoder.into_inner();

        combine_chunks(dest, async_uploader.chunks).await
    }

    #[tokio::test]
    async fn test_uploader() {
        let dest = "dest";
        let count = 1000;

        let all_bytes =
            generate_and_compress_data(dest, count).await.unwrap();

        let mut decoder = ZstdDecoder::new(vec![]);
        decoder.write_all(&all_bytes).await.unwrap();
        decoder.shutdown().await.unwrap();
        let decoded = decoder.into_inner();
        let messages: Vec<Message> =
            serde_json::from_str(&String::from_utf8(decoded).unwrap())
                .unwrap();

        assert_eq!(messages.len(), count);
        std::fs::remove_dir_all(dest).unwrap();
    }
}
