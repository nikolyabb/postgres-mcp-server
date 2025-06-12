from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr

class Settings(BaseSettings):
    DB_NAME: str = Field(description="Postgres Database name", default="postgres")
    DB_USER: str = Field(description="Postgres Database user name", default="postgres")
    DB_PASSWORD: SecretStr = Field(description="Postgres Database password", default=SecretStr("postgres"))
    DB_HOST: str = Field(description="Postgres Database host URL", default="localhost")
    DB_PORT: int = Field(description="Postgres Database port", default=5432)
    
    class Config:
        env_file = ".env"

def get_settings():
    return Settings()