use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct User {
    pub id: String,
    pub token: String,
    pub email: String,
    pub name: String,
}

#[derive(Deserialize, Debug)]
pub struct UserResponse {
    pub user: User,
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id &&
        self.token == other.token &&
        self.email == other.email &&
        self.name == other.name
    }
}

impl Eq for User {}

impl User {
    pub fn dummy() -> User {
        User {
            id: String::from("0123456789"),
            token: String::from("SUPER_SECRET_TOKEN"),
            email: String::from("test@oxen.ai"),
            name: String::from("Dummy User"),
        }
    }
}