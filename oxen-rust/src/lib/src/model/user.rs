use serde::{Deserialize, Serialize};

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
        self.id == other.id
            && self.token == other.token
            && self.email == other.email
            && self.name == other.name
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

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::UserResponse;

    #[test]
    fn test_deserialize() -> Result<(), OxenError> {
        let data = r#"
            {
                "user": {
                    "id": "1234",
                    "name": "Ox",
                    "email": "ox@oxen.ai",
                    "token": "super_secret"
                }
            }
        "#;
        let user: UserResponse = serde_json::from_str(data)?;

        assert_eq!("1234", user.user.id);
        assert_eq!("Ox", user.user.name);
        assert_eq!("ox@oxen.ai", user.user.email);
        assert_eq!("super_secret", user.user.token);
        Ok(())
    }
}
