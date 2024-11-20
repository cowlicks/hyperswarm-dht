use std::{
    collections::VecDeque,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use async_udx::UdxSocket;
use futures::{Future, Sink, Stream};

use crate::{udx::cenc::MsgData, Result};

// Wrapper struct around UdxSocket that handles Messages
#[derive(Debug)]
pub struct MessageDataStream {
    socket: UdxSocket,
    // Buffer for incoming messages that couldn't be processed immediately
    recv_queue: VecDeque<(MsgData, SocketAddr)>,
}

impl MessageDataStream {
    pub fn new(socket: UdxSocket) -> Self {
        Self {
            socket,
            recv_queue: VecDeque::new(),
        }
    }
}

impl Stream for MessageDataStream {
    type Item = Result<(MsgData, SocketAddr)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First check if we have any buffered messages
        if let Some(msg) = self.recv_queue.pop_front() {
            return Poll::Ready(Some(Ok(msg)));
        }

        // Try to receive data from the socket
        let mut fut = self.socket.recv();
        match Pin::new(&mut fut).poll(cx) {
            Poll::Ready(Ok((addr, buff))) => {
                // Try to decode the received message
                match MsgData::decode(&buff) {
                    Ok(message) => Poll::Ready(Some(Ok((message, addr)))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e.into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<(MsgData, SocketAddr)> for MessageDataStream {
    type Error = crate::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: (MsgData, SocketAddr)) -> Result<()> {
        let (message, _addr) = item;

        let buff = MsgData::encode(&message)?;
        self.socket.send(_addr, &buff);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // No buffering in UdxSocket, so no need to flush
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        // No special cleanup needed
        Poll::Ready(Ok(()))
    }
}
