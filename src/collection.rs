use std::{collections::hash_map::Entry, collections::HashMap, iter::FromIterator};
use typed_generational_arena::{Arena, Index, Iter};

use crate::{Error, Id};

pub struct CollectionWithID<T> {
    storage: Arena<T>,
    ids: HashMap<String, Index<T>>,
}

impl<T> Default for CollectionWithID<T> {
    fn default() -> Self {
        CollectionWithID {
            storage: Arena::default(),
            ids: HashMap::default(),
        }
    }
}

impl<T: Id> CollectionWithID<T> {
    pub fn insert(&mut self, o: T) -> Result<Index<T>, Error> {
        let id = o.id().to_owned();
        match self.ids.entry(id) {
            Entry::Occupied(_) => Err(Error::DuplicateStop(o.id().to_owned())),
            Entry::Vacant(e) => {
                let index = self.storage.insert(o);
                e.insert(index);
                Ok(index)
            }
        }
    }
}

impl<T> CollectionWithID<T> {
    pub fn get(&self, i: Index<T>) -> Option<&T> {
        self.storage.get(i)
    }

    pub fn get_by_id(&self, id: &str) -> Option<&T> {
        self.ids.get(id).and_then(|idx| self.storage.get(*idx))
    }

    pub fn get_mut_by_id(&mut self, id: &str) -> Option<&mut T> {
        let idx = self.ids.get(id)?;
        self.storage.get_mut(*idx)
    }

    pub fn get_index(&self, id: &str) -> Option<&Index<T>> {
        self.ids.get(id)
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Iterates over the `(Index<T>, &T)` of the `CollectionWithID`.
    pub fn iter(&self) -> Iter<'_, T> {
        self.storage.iter()
    }
}

impl<T: Id> FromIterator<T> for CollectionWithID<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self::default();

        for i in iter {
            // Note FromIterator does not handle the insertion error
            let _ = c
                .insert(i)
                .map_err(|e| println!("impossible to insert elt: {}", e));
        }

        c
    }
}
