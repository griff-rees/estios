#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
from dataclasses import dataclass
from logging import getLogger
from typing import Final, Union

# from dotenv import load_dotenv
# from fastapi import Depends, FastAPI
# from fastapi.security import OAuth2PasswordRequestForm
# from fastapi_login import LoginManager
# from fastapi_login.exceptions import InvalidCredentialsException

logger = getLogger(__name__)

# load_dotenv()

# SERVER_SECRET_NAME: Final[str] = "SERVER-KEY"
DBPathType = Union[str, bytes, os.PathLike]

DBUserType = dict[str, dict[str, str]]
DBDictType = dict[str, DBUserType]

DB_PATH: Final[DBPathType] = "users_db.json"

# try:
#     SECRET: Final[str] = os.environ[SERVER_SECRET_NAME]
# except KeyError:
#     logger.error(f"{SERVER_SECRET_NAME} access token not found in local .env file.")
#     SECRET: Final[str] = os.urandom(24).hex()
#     logger.warning(f"New {SERVER_SECRET_NAME} generated.")


@dataclass
class AuthDB:

    json_db_path: DBPathType = DB_PATH
    users_key: str = "users"
    # manager = LoginManager(SECRET, '/login')

    def __post_init__(self):
        with open(self.json_db_path) as db:
            self.db = json.load(db)

        # @self.manager.user_loader()
        # def query_user(self, user_id: str) -> str:
        #     """Get a user from the db

        #     :param user_id: E-Mail of the user
        #     :return: None or the user object
        #     """
        #     return self.users.get(user_id)

    @property
    def users(self) -> DBUserType:
        return self.db[self.users_key]

    def add_user(self, user_id: str, name: str, password: str) -> None:
        self.users[user_id] = {"name": name, "password": password}

    def write(self) -> None:
        with open(self.json_db_path, "w") as db_file:
            json.dump(self.db, db_file)


# def set_auth_middleware(app: FastAPI, auth_db: AuthDB) -> FastAPI:
#     return auth_db.manager.useRequest(app)


# def auth_app() -> FastAPI:
#     auth_app = FastAPI()

# server_app.include_router(
#     fastapi_users.get_auth_router(auth_backend),
#     prefix="/auth/jwt",
#     tags=["auth"]
# )
# server_app.include_router(fastapi_users.get_register_router(),
#                           prefix="/auth", tags=["auth"])
# server_app.include_router(
#     fastapi_users.get_reset_password_router(),
#     prefix="/auth",
#     tags=["auth"],
# )
# server_app.include_router(
#     fastapi_users.get_verify_router(),
#     prefix="/auth",
#     tags=["auth"],
# )
# server_app.include_router(fastapi_users.get_users_router(),
#                           prefix="/users", tags=["users"])

# FastAPI-login
# @auth_app.post('/login')
# def login(data: OAuth2PasswordRequestForm = Depends()):
#     email = data.username
#     password = data.password

#     user = query_user(email)
#     if not user:
#         # you can return any response or error of your choice
#         raise InvalidCredentialsException
#     elif password != user['password']:
#         raise InvalidCredentialsException

#     access_token = manager.create_access_token(
#         data={'sub': email}
#     )
#     return {'token': access_token}


# @server_app.get("/authenticated-route")
# async def authenticated_route(user: UserDB = Depends(current_active_user)):
#     return {"message": f"Hello {user.email}!"}

# return auth_app
