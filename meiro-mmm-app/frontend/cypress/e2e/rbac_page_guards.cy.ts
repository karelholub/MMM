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
        journeys_enabled: true,
        access_control_enabled: true,
        custom_roles_enabled: true,
      }
      return cy.request({
        method: 'POST',
        url: '/api/settings',
        headers: adminHeaders,
        body: payload,
      })
    })
}

function provisionUserWithRole(roleName: string): Cypress.Chainable<string> {
  const email = uniqueEmail(`rbac.${roleName.toLowerCase()}`)
  return cy
    .request({ method: 'GET', url: '/api/admin/roles', headers: adminHeaders })
    .then((rolesRes) => {
      expect(rolesRes.status).to.eq(200)
      const role = (rolesRes.body.items as Array<any>).find((r) => r.name === roleName)
      expect(role).to.exist

      return cy
        .request({
          method: 'POST',
          url: '/api/admin/invitations',
          headers: adminHeaders,
          body: {
            email,
            role_id: role.id,
            workspace_id: 'default',
          },
        })
        .then((inviteRes) => {
          expect(inviteRes.status).to.eq(200)
          return cy.request({
            method: 'POST',
            url: '/api/invitations/accept',
            body: {
              token: inviteRes.body.token,
              name: `RBAC ${roleName}`,
            },
          })
        })
        .then((acceptRes) => {
          expect(acceptRes.status).to.eq(200)
          return email
        })
    })
}

describe('RBAC route/page guards', () => {
  beforeEach(() => {
    enableAccessFlags().its('status').should('eq', 200)
  })

  it('custom journeys-view role sees Journeys but is blocked from Alerts and Settings', () => {
    const roleName = `Journeys Viewer ${Date.now()}`

    cy.request({
      method: 'POST',
      url: '/api/admin/roles',
      headers: adminHeaders,
      body: {
        name: roleName,
        description: 'Cypress journeys-only view role',
        permission_keys: ['journeys.view'],
        workspace_id: 'default',
      },
    }).then((createRoleRes) => {
      expect(createRoleRes.status).to.eq(200)

      const email = uniqueEmail('rbac.journeysonly')
      cy.request({ method: 'POST', url: '/api/auth/logout' })

      cy.request({
        method: 'POST',
        url: '/api/admin/invitations',
        headers: adminHeaders,
        body: {
          email,
          role_id: createRoleRes.body.id,
          workspace_id: 'default',
        },
      }).then((inviteRes) => {
        expect(inviteRes.status).to.eq(200)

        cy.request({
          method: 'POST',
          url: '/api/invitations/accept',
          body: { token: inviteRes.body.token, name: 'Journeys only user' },
        }).then((acceptRes) => {
          expect(acceptRes.status).to.eq(200)

          cy.request({ method: 'POST', url: '/api/auth/login', body: { email, workspace_id: 'default' } }).then((loginRes) => {
            expect(loginRes.status).to.eq(200)

            cy.visit('/analytics/journeys')
            cy.contains('Journeys').should('be.visible')
            cy.contains('button', 'Create journey').should('be.disabled')

            cy.visit('/alerts')
            cy.contains('No access').should('be.visible')

            cy.visit('/settings')
            cy.contains('No access').should('be.visible')
          })
        })
      })
    })
  })

  it('editor can create journey definitions from Journeys page', () => {
    provisionUserWithRole('Editor').then((email) => {
      cy.request({ method: 'POST', url: '/api/auth/logout' })
      cy.request({ method: 'POST', url: '/api/auth/login', body: { email, workspace_id: 'default' } }).then((loginRes) => {
        expect(loginRes.status).to.eq(200)

        cy.visit('/analytics/journeys')
        cy.contains('Journeys').should('be.visible')
        cy.contains('button', 'Create journey').should('be.enabled')
      })
    })
  })
})
