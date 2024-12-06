use std::mem::swap;
use tokio::sync::broadcast::{self, Receiver, Sender};

const CHANNEL_SIZE: usize = 32;

#[derive(Debug)]
pub struct Master<T> {
    value: T,
    tx: Sender<T>,
}

#[derive(Debug)]
pub struct Slave<T: Clone> {
    current: T,
    rx: Receiver<T>,
}

impl<T: Clone> Master<T> {
    pub fn new(initial: T) -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_SIZE);
        Self { value: initial, tx }
    }

    /// Set the inner value. Return the old.
    pub fn set(&mut self, new: T) -> T {
        let mut out = new.clone();
        swap(&mut self.value, &mut out);
        let _ = self.tx.send(new);
        out
    }

    pub fn get(&self) -> T {
        self.value.clone()
    }
    pub fn get_ref(&self) -> &T {
        &self.value
    }

    pub fn view(&self) -> Slave<T> {
        Slave {
            current: self.value.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

impl<T: Clone> Slave<T> {
    pub fn update(&mut self) -> Vec<T> {
        let mut out = vec![];
        loop {
            if let Ok(new) = self.rx.try_recv() {
                let mut x = new.clone();
                swap(&mut self.current, &mut x);
                out.push(x);
            } else {
                return out;
            }
        }
    }

    pub fn get(&self) -> &T {
        &self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update() {
        let mut model = Master::new([0u8; 32]);
        let mut view = model.view();

        for i in 1..=3 {
            model.set([i; 32]);
            assert!(!view.update().is_empty());
            assert_eq!(view.get(), &[i; 32]);
        }
    }

    #[tokio::test]
    async fn test_lagged_updates() {
        let mut model = Master::new([0u8; 32]);
        let mut view = model.view();

        let max = CHANNEL_SIZE + 5;
        // Make multiple rapid updates
        for i in 1..=max {
            model.set([i as u8; 32]);
        }

        loop {
            let x = view.update();
            if !x.is_empty() {
                break;
            }
        }
        assert_eq!(view.get(), &[max as u8; 32]);
    }
}
