from typing import Any, Callable

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response

from app.modules.auth_access.schemas import (
    AuthLoginPayload,
    AuthWorkspaceSwitchPayload,
    DatasourceCredentialUpdate,
    OAuthSelectAccountsPayload,
    OAuthStartPayload,
)
from app.services_access_control import DEFAULT_WORKSPACE_ID
from app.services_auth import (
    CSRF_HEADER_NAME,
    SESSION_COOKIE_NAME,
    authenticate_local_user,
    create_session,
    ensure_local_password_seed_users,
    ensure_user_and_membership,
    issue_csrf_token,
    require_auth_context,
    resolve_auth_context,
    revoke_all_user_sessions,
    revoke_session,
)
from app.services_oauth_connections import (
    OAUTH_PROVIDER_LABELS,
    build_authorization_url,
    complete_oauth_callback,
    create_oauth_session,
    disconnect_connection,
    list_oauth_connections,
    list_provider_accounts,
    normalize_provider_key,
    select_accounts,
    test_connection_health,
)
from app.models_config_dq import WorkspaceMembership


def create_router(
    *,
    get_db_dependency: Callable[..., Any],
    require_permission_dependency: Callable[[str], Callable[..., Any]],
    get_base_url_fn: Callable[[], str],
    get_frontend_url_fn: Callable[[], str],
    get_connected_platforms_fn: Callable[[], list[str]],
    meiro_connected_fn: Callable[[], bool],
    delete_token_fn: Callable[[str], bool],
    datasource_config_obj: Any,
    session_cookie_secure: bool,
    session_cookie_samesite: str,
    session_cookie_max_age: int,
) -> APIRouter:
    router = APIRouter(tags=["auth_access"])

    def _set_session_cookie(response: Response, session_id: str) -> None:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_id,
            max_age=session_cookie_max_age,
            httponly=True,
            secure=session_cookie_secure,
            samesite=session_cookie_samesite,
            path="/",
        )

    def _clear_session_cookie(response: Response) -> None:
        response.delete_cookie(
            key=SESSION_COOKIE_NAME,
            path="/",
            secure=session_cookie_secure,
            samesite=session_cookie_samesite,
        )

    def _resolve_workspace_user_from_request(request: Request, db) -> tuple[str, str]:
        raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if raw_session_id:
            ctx = require_auth_context(db, request)
            return ctx.workspace.id, ctx.user.id
        workspace_id = (request.headers.get("X-Workspace-Id") or request.query_params.get("workspace_id") or "default").strip() or "default"
        user_id = (request.headers.get("X-User-Id") or request.query_params.get("user_id") or "system").strip() or "system"
        return workspace_id, user_id

    def _oauth_redirect_uri(provider_key: str) -> str:
        return f"{get_base_url_fn()}/oauth/{provider_key}/callback"

    @router.post("/api/auth/login")
    def login_with_session(payload: AuthLoginPayload, response: Response, request: Request, db=Depends(get_db_dependency)):
        provider = (payload.provider or "bootstrap").strip().lower()
        workspace_id = (payload.workspace_id or DEFAULT_WORKSPACE_ID).strip() or DEFAULT_WORKSPACE_ID
        if provider not in {"bootstrap", "local_password"}:
            raise HTTPException(status_code=400, detail="Unsupported auth provider")

        user = None
        if provider == "local_password":
            identifier = (payload.username or payload.email or "").strip().lower()
            if not identifier:
                raise HTTPException(status_code=400, detail="username or email is required")
            if not payload.password:
                raise HTTPException(status_code=400, detail="password is required")
            ensure_local_password_seed_users(db, workspace_id=workspace_id)
            user = authenticate_local_user(
                db,
                identifier=identifier,
                password=payload.password,
                workspace_id=workspace_id,
            )
            if not user:
                raise HTTPException(status_code=401, detail="Invalid credentials")
        else:
            email = (payload.email or "").strip()
            if not email:
                raise HTTPException(status_code=400, detail="email is required")
            try:
                user = ensure_user_and_membership(
                    db,
                    email=email,
                    name=payload.name,
                    workspace_id=workspace_id,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

        session_tokens = create_session(
            db,
            user=user,
            workspace_id=workspace_id,
            request=request,
        )
        _set_session_cookie(response, session_tokens["session_id"])
        ctx = resolve_auth_context(db, raw_session_id=session_tokens["session_id"])
        if not ctx:
            raise HTTPException(status_code=500, detail="Failed to establish session")
        return {
            "user": {
                "id": ctx.user.id,
                "email": ctx.user.email,
                "name": ctx.user.name,
                "status": ctx.user.status,
            },
            "workspace": {
                "id": ctx.workspace.id,
                "name": ctx.workspace.name,
                "slug": ctx.workspace.slug,
            },
            "role": ctx.role.name if ctx.role else None,
            "permissions": sorted(ctx.permissions),
            "csrf_token": session_tokens["csrf_token"],
            "provider": provider,
        }

    @router.get("/api/auth/providers")
    def auth_providers():
        return {
            "providers": [
                {"id": "local_password", "label": "Username & Password", "enabled": True},
                {"id": "google_oauth", "label": "Google", "enabled": False, "coming_soon": True},
                {"id": "sso_oidc", "label": "SSO (OIDC/SAML)", "enabled": False, "coming_soon": True},
            ]
        }

    @router.get("/api/auth/me")
    def get_auth_me(request: Request, db=Depends(get_db_dependency)):
        ctx = require_auth_context(db, request)
        csrf_token = issue_csrf_token(
            db,
            request.cookies.get(SESSION_COOKIE_NAME),
            request.headers.get(CSRF_HEADER_NAME),
        )
        return {
            "authenticated": True,
            "user": {
                "id": ctx.user.id,
                "username": getattr(ctx.user, "username", None),
                "email": ctx.user.email,
                "name": ctx.user.name,
                "status": ctx.user.status,
                "last_login_at": ctx.user.last_login_at,
            },
            "workspace": {
                "id": ctx.workspace.id,
                "name": ctx.workspace.name,
                "slug": ctx.workspace.slug,
            },
            "membership": {
                "id": ctx.membership.id,
                "status": ctx.membership.status,
                "role_id": ctx.membership.role_id,
                "role_name": ctx.role.name if ctx.role else None,
            },
            "permissions": sorted(ctx.permissions),
            "csrf_token": csrf_token,
        }

    @router.post("/api/auth/workspace")
    def switch_auth_workspace(
        payload: AuthWorkspaceSwitchPayload,
        request: Request,
        response: Response,
        db=Depends(get_db_dependency),
    ):
        current_ctx = require_auth_context(db, request)
        membership = (
            db.query(WorkspaceMembership)
            .filter(
                WorkspaceMembership.workspace_id == payload.workspace_id,
                WorkspaceMembership.user_id == current_ctx.user.id,
                WorkspaceMembership.status == "active",
            )
            .first()
        )
        if not membership:
            raise HTTPException(status_code=403, detail="No active membership for requested workspace")
        revoke_session(db, request.cookies.get(SESSION_COOKIE_NAME) or "")
        tokens = create_session(
            db,
            user=current_ctx.user,
            workspace_id=payload.workspace_id,
            request=request,
        )
        _set_session_cookie(response, tokens["session_id"])
        next_ctx = resolve_auth_context(db, raw_session_id=tokens["session_id"])
        if not next_ctx:
            raise HTTPException(status_code=500, detail="Failed to switch workspace")
        return {
            "workspace": {
                "id": next_ctx.workspace.id,
                "name": next_ctx.workspace.name,
                "slug": next_ctx.workspace.slug,
            },
            "role": next_ctx.role.name if next_ctx.role else None,
            "permissions": sorted(next_ctx.permissions),
            "csrf_token": tokens["csrf_token"],
        }

    @router.post("/api/auth/logout")
    def logout_session(request: Request, response: Response, db=Depends(get_db_dependency)):
        raw = request.cookies.get(SESSION_COOKIE_NAME)
        if raw:
            revoke_session(db, raw)
        _clear_session_cookie(response)
        return {"ok": True}

    @router.post("/api/auth/logout-all")
    def logout_all_sessions(request: Request, response: Response, db=Depends(get_db_dependency)):
        ctx = require_auth_context(db, request)
        revoked = revoke_all_user_sessions(db, ctx.user.id)
        _clear_session_cookie(response)
        return {"ok": True, "sessions_revoked": revoked}

    @router.get("/api/auth/status")
    def auth_status():
        connected = get_connected_platforms_fn()
        if meiro_connected_fn() and "meiro_cdp" not in connected:
            connected.append("meiro_cdp")
        return {"connected": connected}

    @router.get("/api/connections")
    def api_list_connections(
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
        return list_oauth_connections(db, workspace_id=workspace_id)

    @router.post("/api/connections/{provider}/start")
    def api_start_connection(
        provider: str,
        body: OAuthStartPayload = Body(default=OAuthStartPayload()),
        request: Request = None,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        provider_key = normalize_provider_key(provider)
        if provider_key not in OAUTH_PROVIDER_LABELS:
            raise HTTPException(status_code=404, detail="Unsupported provider")
        workspace_id, user_id = _resolve_workspace_user_from_request(request, db)
        try:
            session_data = create_oauth_session(
                db,
                workspace_id=workspace_id,
                user_id=user_id,
                provider_key=provider_key,
                return_url=body.return_url,
            )
            auth_url = build_authorization_url(
                provider_key=provider_key,
                state=session_data["state"],
                code_challenge=session_data["code_challenge"],
                redirect_uri=_oauth_redirect_uri(provider_key),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"authorization_url": auth_url}

    @router.post("/api/connections/{provider}/reauth")
    def api_reauth_connection(
        provider: str,
        body: OAuthStartPayload = Body(default=OAuthStartPayload()),
        request: Request = None,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        return api_start_connection(provider=provider, body=body, request=request, db=db)

    @router.get("/oauth/{provider}/callback")
    def oauth_callback(
        provider: str,
        code: str | None = Query(None),
        state: str | None = Query(None),
        error: str | None = Query(None),
        db=Depends(get_db_dependency),
    ):
        provider_key = normalize_provider_key(provider)
        frontend_url = get_frontend_url_fn()
        if provider_key not in OAUTH_PROVIDER_LABELS:
            return RedirectResponse(url=f"{frontend_url}/datasources?oauth_error=unsupported_provider")

        if error:
            return RedirectResponse(url=f"{frontend_url}/datasources?oauth_provider={provider_key}&oauth_error={error}")
        if not code or not state:
            return RedirectResponse(url=f"{frontend_url}/datasources?oauth_provider={provider_key}&oauth_error=missing_code_or_state")
        try:
            connection, return_url, normalized_error = complete_oauth_callback(
                db,
                provider_key=provider_key,
                code=code,
                state=state,
                redirect_uri=_oauth_redirect_uri(provider_key),
            )
            if normalized_error:
                message = normalized_error.message.replace(" ", "+")
                return RedirectResponse(
                    url=f"{frontend_url}/datasources?oauth_provider={provider_key}&oauth_status=error&oauth_error={normalized_error.code}&oauth_message={message}"
                )
            redirect_base = f"{frontend_url}/datasources"
            if return_url and (return_url.startswith("/") or return_url.startswith(frontend_url)):
                redirect_base = f"{frontend_url}{return_url}" if return_url.startswith("/") else return_url
            return RedirectResponse(
                url=f"{redirect_base}?oauth_provider={provider_key}&oauth_status=connected&accounts={len((connection.config_json or {}).get('available_accounts') or [])}"
            )
        except Exception:
            return RedirectResponse(url=f"{frontend_url}/datasources?oauth_provider={provider_key}&oauth_status=error&oauth_error=callback_failed")

    @router.get("/api/connections/{provider}/accounts")
    def api_connection_accounts(
        provider: str,
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.view")),
    ):
        provider_key = normalize_provider_key(provider)
        workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
        try:
            accounts = list_provider_accounts(db, workspace_id=workspace_id, provider_key=provider_key)
            return {"provider_key": provider_key, "accounts": accounts}
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.post("/api/connections/{provider}/select-accounts")
    def api_connection_select_accounts(
        provider: str,
        body: OAuthSelectAccountsPayload,
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        provider_key = normalize_provider_key(provider)
        workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
        try:
            return select_accounts(
                db,
                workspace_id=workspace_id,
                provider_key=provider_key,
                account_ids=body.account_ids,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @router.post("/api/connections/{provider}/test")
    def api_connection_test(
        provider: str,
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        provider_key = normalize_provider_key(provider)
        workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
        try:
            return test_connection_health(db, workspace_id=workspace_id, provider_key=provider_key)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @router.post("/api/connections/{provider}/disconnect")
    def api_connection_disconnect(
        provider: str,
        request: Request,
        db=Depends(get_db_dependency),
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        provider_key = normalize_provider_key(provider)
        workspace_id, _user_id = _resolve_workspace_user_from_request(request, db)
        try:
            out = disconnect_connection(db, workspace_id=workspace_id, provider_key=provider_key)
            return {"ok": True, "connection": out}
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    @router.delete("/api/auth/{platform}")
    def disconnect_platform(platform: str):
        if delete_token_fn(platform):
            return {"message": f"Disconnected {platform}"}
        raise HTTPException(status_code=404, detail=f"No connection found for {platform}")

    @router.get("/api/admin/datasource-config")
    def get_datasource_config_status(
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        return datasource_config_obj.get_status()

    @router.post("/api/admin/datasource-config")
    def update_datasource_config(
        body: DatasourceCredentialUpdate,
        _ctx=Depends(require_permission_dependency("settings.manage")),
    ):
        platform = body.platform.lower()
        if platform not in ("google", "meta", "linkedin"):
            raise HTTPException(status_code=400, detail="platform must be google, meta, or linkedin")
        try:
            if platform == "google":
                if body.client_id is not None:
                    datasource_config_obj.set_stored("google", client_id=body.client_id)
                if body.client_secret is not None:
                    datasource_config_obj.set_stored("google", client_secret=body.client_secret)
                if body.developer_token is not None:
                    datasource_config_obj.set_stored("google", developer_token=body.developer_token)
            elif platform == "meta":
                if body.app_id is not None:
                    datasource_config_obj.set_stored("meta", app_id=body.app_id)
                if body.app_secret is not None:
                    datasource_config_obj.set_stored("meta", app_secret=body.app_secret)
            elif platform == "linkedin":
                if body.client_id is not None:
                    datasource_config_obj.set_stored("linkedin", client_id=body.client_id)
                if body.client_secret is not None:
                    datasource_config_obj.set_stored("linkedin", client_secret=body.client_secret)
            return {
                "message": "Credentials updated",
                "platform": platform,
                "configured": datasource_config_obj.get_platform_configured(platform),
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    @router.get("/api/auth/{platform}")
    def start_oauth(platform: str, request: Request, db=Depends(get_db_dependency)):
        provider_key = normalize_provider_key(platform)
        if provider_key not in OAUTH_PROVIDER_LABELS:
            raise HTTPException(status_code=400, detail=f"Unknown platform: {platform}")
        workspace_id, user_id = _resolve_workspace_user_from_request(request, db)
        try:
            session_data = create_oauth_session(
                db,
                workspace_id=workspace_id,
                user_id=user_id,
                provider_key=provider_key,
                return_url="/datasources",
            )
            auth_url = build_authorization_url(
                provider_key=provider_key,
                state=session_data["state"],
                code_challenge=session_data["code_challenge"],
                redirect_uri=_oauth_redirect_uri(provider_key),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return RedirectResponse(url=auth_url)

    return router
