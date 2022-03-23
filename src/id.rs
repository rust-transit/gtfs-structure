use core::marker::PhantomData;
#[derive(Derivative)]
#[derive(Serialize, Deserialize)]
#[derivative(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Id<T> {
    id: String,
    #[serde(skip)]
    #[derivative(Debug(bound = ""))]
    #[derivative(Debug = "ignore")]
    #[derivative(Clone(bound = ""))]
    #[derivative(Eq(bound = ""))]
    #[derivative(PartialEq(bound = ""))]
    #[derivative(Hash(bound = ""))]
    _phantom: PhantomData<T>,
}

impl<T> Id<T> {
    pub fn must_exists(s: String) -> Id<T> {
        Id {
            id: s,
            _phantom: PhantomData,
        }
    }
}

impl<T> std::ops::Deref for Id<T> {
    type Target = str;
    fn deref(&self) -> &str {
        &self.id
    }
}
