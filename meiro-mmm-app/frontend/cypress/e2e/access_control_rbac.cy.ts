const adminHeaders = {
  'X-User-Role': 'admin',
  'X-User-Id': 'cypress-admin',
}

function uniqueEmail(prefix: string): string {
  const now = Date.now()
  const rand = Math.floor(Math.random() * 1_000_000)
  return `${prefix}.${now}.${rand}@example.com`
}

function enableAccessFlags(): Cypress.Chainable {
  return cy
    .request({
      method: 'GET',
      url: '/api/settings',
      headers: adminHeaders,
    })
    .then((settingsRes) => {
      expect(settingsRes.status).to.eq(200)
      const payload = settingsRes.body
      payload.feature_flags = {
        ...(payload.feature_flags || {}),
        access_control_enabled: true,
        custom_roles_enabled: true,
        audit_log_enabled: true,
      }
      return cy.request({
        method: 'POST',
        url: '/api/settings',
        headers: adminHeaders,
        body: payload,
      })
    })
}

describe('Access Control RBAC critical flows', () => {
  beforeEach(() => {
    enableAccessFlags().its('status').should('eq', 200)
  })

  it('admin invites a user, user accepts invite, becomes Viewer', () => {
    const email = uniqueEmail('rbac.viewer')

    cy.request({ method: 'GET', url: '/api/admin/roles', headers: adminHeaders }).then((rolesRes) => {
      expect(rolesRes.status).to.eq(200)
      const viewerRole = (rolesRes.body.items as Array<any>).find((r) => r.name === 'Viewer')
      expect(viewerRole).to.exist

      cy.request({
        method: 'POST',
        url: '/api/admin/invitations',
        headers: adminHeaders,
        body: {
          email,
          role_id: viewerRole.id,
          workspace_id: 'default',
        },
      }).then((inviteRes) => {
        expect(inviteRes.status).to.eq(200)
        expect(inviteRes.body.token).to.be.a('string')

        cy.request({
          method: 'POST',
          url: '/api/invitations/accept',
          body: {
            token: inviteRes.body.token,
            name: 'RBAC Viewer',
          },
        }).then((acceptRes) => {
          expect(acceptRes.status).to.eq(200)
          expect(acceptRes.body.ok).to.eq(true)

          cy.request({
            method: 'GET',
            url: '/api/admin/users',
            headers: adminHeaders,
            qs: { search: email, workspaceId: 'default' },
          }).then((usersRes) => {
            expect(usersRes.status).to.eq(200)
            expect(usersRes.body.items).to.have.length.greaterThan(0)
            expect(usersRes.body.items[0].email).to.eq(email)
            expect(usersRes.body.items[0].role_name).to.eq('Viewer')
          })
        })
      })
    })
  })

  it('viewer cannot access admin access-control endpoints (denial contract)', () => {
    const email = uniqueEmail('rbac.denied.viewer')

    cy.request({ method: 'POST', url: '/api/auth/login', body: { email, workspace_id: 'default' } }).then((loginRes) => {
      expect(loginRes.status).to.eq(200)

      cy.request({
        method: 'GET',
        url: '/api/admin/users',
        failOnStatusCode: false,
      }).then((denyUsers) => {
        expect(denyUsers.status).to.eq(403)
        expect(denyUsers.body.detail.code).to.eq('permission_denied')
        expect(denyUsers.body.detail.permission).to.eq('users.manage')
      })

      cy.request({
        method: 'GET',
        url: '/api/admin/roles',
        failOnStatusCode: false,
      }).then((denyRoles) => {
        expect(denyRoles.status).to.eq(403)
        expect(denyRoles.body.detail.code).to.eq('permission_denied')
        expect(denyRoles.body.detail.permission).to.eq('roles.manage')
      })
    })
  })

  it('admin creates custom role, assigns it, and audit log records role change', () => {
    const email = uniqueEmail('rbac.role.member')
    const roleName = `Campaign Analyst ${Date.now()}`

    cy.request({ method: 'POST', url: '/api/auth/logout' })

    cy.request({ method: 'GET', url: '/api/admin/roles', headers: adminHeaders }).then((rolesRes) => {
      expect(rolesRes.status).to.eq(200)
      const viewerRole = (rolesRes.body.items as Array<any>).find((r) => r.name === 'Viewer')
      expect(viewerRole).to.exist

      cy.request({
        method: 'POST',
        url: '/api/admin/roles',
        headers: adminHeaders,
        body: {
          name: roleName,
          description: 'Cypress limited campaign analytics role',
          permission_keys: ['journeys.view', 'funnels.view', 'attribution.view', 'alerts.view'],
          workspace_id: 'default',
        },
      }).then((createRoleRes) => {
        expect(createRoleRes.status).to.eq(200)
        const customRoleId = createRoleRes.body.id
        expect(customRoleId).to.be.a('string')

        cy.request({
          method: 'POST',
          url: '/api/admin/invitations',
          headers: adminHeaders,
          body: {
            email,
            role_id: viewerRole.id,
            workspace_id: 'default',
          },
        }).then((inviteRes) => {
          expect(inviteRes.status).to.eq(200)

          cy.request({
            method: 'POST',
            url: '/api/invitations/accept',
            body: { token: inviteRes.body.token, name: 'Campaign Analyst User' },
          }).then((acceptRes) => {
            expect(acceptRes.status).to.eq(200)
            const membershipId = acceptRes.body.membership_id
            expect(membershipId).to.be.a('string')

            cy.request({
              method: 'PATCH',
              url: `/api/admin/memberships/${membershipId}`,
              headers: adminHeaders,
              body: { role_id: customRoleId },
            }).then((patchRes) => {
              expect(patchRes.status).to.eq(200)
              expect(patchRes.body.role_id).to.eq(customRoleId)

              cy.request({ method: 'POST', url: '/api/auth/login', body: { email, workspace_id: 'default' } }).then((memberLogin) => {
                expect(memberLogin.status).to.eq(200)

                cy.request({ method: 'GET', url: '/api/auth/me' }).then((meRes) => {
                  expect(meRes.status).to.eq(200)
                  const perms = new Set<string>(meRes.body.permissions || [])
                  expect(perms.has('journeys.view')).to.eq(true)
                  expect(perms.has('settings.manage')).to.eq(false)
                  expect(perms.has('users.manage')).to.eq(false)
                })
              })

              cy.request({ method: 'POST', url: '/api/auth/logout' })

              cy.request({
                method: 'GET',
                url: '/api/admin/audit-log',
                headers: adminHeaders,
                qs: {
                  workspaceId: 'default',
                  action: 'membership.role_changed',
                  actor: 'cypress-admin',
                },
              }).then((auditRes) => {
                expect(auditRes.status).to.eq(200)
                expect(auditRes.body.items.length).to.be.greaterThan(0)
                const found = (auditRes.body.items as Array<any>).some(
                  (item) => item.action_key === 'membership.role_changed' && item.target_id === membershipId,
                )
                expect(found).to.eq(true)
              })
            })
          })
        })
      })
    })
  })
})
