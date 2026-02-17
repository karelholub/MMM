from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models_config_dq import (
    Permission,
    Role,
    RolePermission,
    User,
    Workspace,
    WorkspaceMembership,
)
from app.services_access_control import (
    DEFAULT_WORKSPACE_ID,
    SYSTEM_ROLE_PERMISSIONS,
    ensure_access_control_seed_data,
)


def _unit_db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_seed_permissions_and_system_roles_idempotent():
    db = _unit_db_session()
    try:
        out1 = ensure_access_control_seed_data(db)
        out2 = ensure_access_control_seed_data(db)

        assert out1["permissions_inserted"] >= 1
        assert out2["permissions_inserted"] == 0

        perms = db.query(Permission).all()
        assert len(perms) >= len({p for v in SYSTEM_ROLE_PERMISSIONS.values() for p in v})

        roles = db.query(Role).filter(Role.is_system == True).all()  # noqa: E712
        role_names = {r.name for r in roles}
        assert {"Admin", "Analyst", "Viewer"}.issubset(role_names)

        rp_count = db.query(RolePermission).count()
        assert rp_count >= len(SYSTEM_ROLE_PERMISSIONS["Viewer"])
    finally:
        db.close()


def test_workspace_membership_unique_workspace_user():
    db = _unit_db_session()
    try:
        ensure_access_control_seed_data(db)
        ws = db.get(Workspace, DEFAULT_WORKSPACE_ID)
        assert ws is not None

        user = User(
            id="u-1",
            email="member@example.com",
            name="Member",
            status="active",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(user)
        db.flush()

        role = db.query(Role).filter(Role.name == "Viewer", Role.is_system == True).first()  # noqa: E712
        assert role is not None

        m1 = WorkspaceMembership(
            id="m-1",
            workspace_id=ws.id,
            user_id=user.id,
            role_id=role.id,
            status="active",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(m1)
        db.commit()

        m2 = WorkspaceMembership(
            id="m-2",
            workspace_id=ws.id,
            user_id=user.id,
            role_id=role.id,
            status="active",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(m2)
        try:
            db.commit()
            assert False, "Expected unique constraint violation"
        except IntegrityError:
            db.rollback()
    finally:
        db.close()

