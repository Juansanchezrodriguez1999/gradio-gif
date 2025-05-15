import os
import random
import string
from typing import Annotated

import gradio as gr
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import APIKeyQuery
from passlib.context import CryptContext
from sqlmodel import Field, Session, SQLModel, create_engine, select

from app.database import User, create_db_and_tables, engine
from app.interface import io
from app.schema import schema

load_dotenv()

X_API_KEY = os.getenv("API_KEY", "s3cr3t")
SCRIPT_NAME = os.getenv("SCRIPT_NAME", "")

app = FastAPI(root_path=SCRIPT_NAME)

query_scheme = APIKeyQuery(name="x_api_key")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def get_password() -> str:
    length = random.randint(8, 32)
    characters = string.ascii_letters + string.digits
    password = "".join(random.choice(characters) for i in range(length))
    return password


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def authenticate_user(username: str, password: str) -> bool:
    '''with Session(engine) as session:
        statement = select(User).where(User.username == username)
        user = session.exec(statement).first()
    if not user:
        return False
    if not verify_password(password, user.password):
        return False'''
    return True


@app.on_event("startup")
def on_startup():
    create_db_and_tables()


@app.get("/json", response_model=dict)
def new_user(request: Request, api_key: str = Depends(query_scheme)) -> dict:
    if api_key != X_API_KEY:
        raise HTTPException(status_code=401, detail="Not authorized")

    username = "user-" + "".join(random.choices("0123456789", k=4))
    password = get_password()

    hashed_password = get_password_hash(password)
    db_user = User(username=username, password=hashed_password)
    with Session(engine) as session:
        session.add(db_user)
        session.commit()
        session.refresh(db_user)

    url = str(request.base_url)
    data = schema.copy()
    data["jsonforms:data"]["username"] = username
    data["jsonforms:data"]["password"] = password
    data["embed"] = url

    return data


app = gr.mount_gradio_app(
    app, io, path="", root_path=SCRIPT_NAME, auth=authenticate_user
)
