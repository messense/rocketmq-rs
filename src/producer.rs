pub trait Producer {
    fn start(&self);

    fn shutdown(&self);
}