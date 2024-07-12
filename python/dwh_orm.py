from typing import Optional
from sqlalchemy import BigInteger, Date
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from datetime import datetime, date


class Base(DeclarativeBase):
    pass

class Publications(Base):
    __tablename__ = "fact_publications"

    published_date_id: Mapped[int] = mapped_column(primary_key=True)
    published_date: Mapped[date] = mapped_column(Date)
    list_id: Mapped[int] = mapped_column(primary_key=True)
    book_sk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    publisher: Mapped[str]
    bestsellers_date_id: Mapped[int]
    bestsellers_date: Mapped[date] = mapped_column(Date)
    published_date_description: Mapped[Optional[str]]
    rank: Mapped[int]
    rank_last_week: Mapped[Optional[int]]
    weeks_on_list: Mapped[Optional[int]]

    """
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")
    """
    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"

class Lists(Base):
    __tablename__ = "dim_lists"

    list_id: Mapped[int] = mapped_column(primary_key=True)
    list_name: Mapped[str]
    list_name_encoded: Mapped[str]
    display_name: Mapped[str]
    updated: Mapped[str]
    list_image: Mapped[Optional[str]]
    list_image_width: Mapped[Optional[str]]
    list_image_height: Mapped[Optional[str]]

    """
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]
    addresses: Mapped[List["Address"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"
    """

class Books(Base):
    __tablename__ = "dim_books"

    book_sk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    title: Mapped[str]
    author: Mapped[str]
    primary_isbn10: Mapped[Optional[str]]
    primary_isbn13: Mapped[Optional[str]]
    description: Mapped[Optional[str]]
    contributor: Mapped[str]
    contributor_note: Mapped[str]
    created_date: Mapped[datetime]
    updated_date: Mapped[datetime]
    price: Mapped[float]
    age_group: Mapped[Optional[str]]
    book_image: Mapped[Optional[str]]
    book_image_width: Mapped[Optional[str]]
    book_image_height: Mapped[Optional[str]]

    """
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id: Mapped[int] = mapped_column(ForeignKey("user_account.id"))
    user: Mapped["User"] = relationship(back_populates="addresses")
    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"
    """
