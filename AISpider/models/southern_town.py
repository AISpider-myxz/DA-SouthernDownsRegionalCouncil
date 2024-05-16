from sqlalchemy import Column, String, Text, Integer, DateTime, func, UniqueConstraint
from .metadata_base import Base


class Southern_Town(Base):
    __tablename__ = 'southern_town'
    id = Column(Integer, primary_key=True)

    application_id = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    application_group = Column(String(255), nullable=True, server_default=None)

    category = Column(String(255), nullable=True, server_default=None)
    sub_category = Column(String(255), nullable=True, server_default=None)

    lodgement_date = Column(Integer, nullable=True,)
    stage = Column(String(255), nullable=True, server_default=None)
    certifier_approval_date = Column(Integer, nullable=True,)

    names = Column(String(255), nullable=True, server_default=None)
    address = Column(Text, nullable=True, server_default=None)
    
    documents = Column(Text, nullable=True, server_default=None)

    _tid = Column(String(255), nullable=False,server_default=None)
    
    created_at = Column(DateTime, server_default=func.now())
    update_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

   