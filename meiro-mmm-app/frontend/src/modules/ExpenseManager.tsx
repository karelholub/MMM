import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface ExpenseEntry {
  id: string
  channel: string
  amount: number
  period: string | null
  notes: string | null
}

interface ExpenseSummary {
  by_channel: Record<string, number>
  total: number
  channels: string[]
}

const card = {
  backgroundColor: '#fff',
  borderRadius: 12,
  border: '1px solid #e9ecef',
  padding: 24,
  boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
} as const

const CHANNEL_OPTIONS = [
  'google_ads', 'meta_ads', 'linkedin_ads', 'email', 'whatsapp',
  'tiktok_ads', 'twitter_ads', 'sms', 'direct_mail', 'other',
]

const COLORS: Record<string, string> = {
  google_ads: '#4285F4', meta_ads: '#1877F2', linkedin_ads: '#0A66C2',
  email: '#28a745', whatsapp: '#25D366', tiktok_ads: '#000000',
  twitter_ads: '#1DA1F2', sms: '#fd7e14', direct_mail: '#6c757d', other: '#adb5bd',
}

export default function ExpenseManager() {
  const queryClient = useQueryClient()
  const [newChannel, setNewChannel] = useState('google_ads')
  const [newAmount, setNewAmount] = useState('')
  const [newPeriod, setNewPeriod] = useState('')
  const [newNotes, setNewNotes] = useState('')

  const expensesQuery = useQuery<ExpenseEntry[]>({
    queryKey: ['expenses'],
    queryFn: async () => {
      const res = await fetch('/api/expenses')
      if (!res.ok) throw new Error('Failed to fetch expenses')
      return res.json()
    },
  })

  const summaryQuery = useQuery<ExpenseSummary>({
    queryKey: ['expenses-summary'],
    queryFn: async () => {
      const res = await fetch('/api/expenses/summary')
      if (!res.ok) throw new Error('Failed to fetch summary')
      return res.json()
    },
  })

  const addMutation = useMutation({
    mutationFn: async (entry: { channel: string; amount: number; period: string; notes: string }) => {
      const res = await fetch('/api/expenses', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(entry),
      })
      if (!res.ok) throw new Error('Failed to add expense')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
      setNewAmount('')
      setNewNotes('')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const res = await fetch(`/api/expenses/${id}`, { method: 'DELETE' })
      if (!res.ok) throw new Error('Failed to delete')
      return res.json()
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['expenses'] })
      queryClient.invalidateQueries({ queryKey: ['expenses-summary'] })
    },
  })

  const handleAdd = () => {
    const amount = parseFloat(newAmount)
    if (isNaN(amount) || amount <= 0) return
    addMutation.mutate({ channel: newChannel, amount, period: newPeriod || '', notes: newNotes })
  }

  const summary = summaryQuery.data
  const expenses = expensesQuery.data || []
  const chartData = summary ? Object.entries(summary.by_channel).map(([channel, amount]) => ({
    channel, amount, fill: COLORS[channel] || '#6c757d',
  })).sort((a, b) => b.amount - a.amount) : []

  return (
    <div>
      <h2 style={{ margin: '0 0 8px', fontSize: '22px', fontWeight: '700', color: '#212529' }}>Expense Management</h2>
      <p style={{ margin: '0 0 24px', fontSize: '14px', color: '#6c757d' }}>
        Track marketing spend across channels. Expenses from ad platform APIs are auto-imported; add manual entries for email, WhatsApp, and other channels.
      </p>

      {/* Summary Cards */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: 16, marginBottom: 24 }}>
        <div style={{ ...card, textAlign: 'center' }}>
          <div style={{ fontSize: '12px', fontWeight: '600', color: '#6c757d', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
            Total Spend
          </div>
          <div style={{ fontSize: '36px', fontWeight: '700', color: '#dc3545', marginTop: 8 }}>
            ${summary?.total.toLocaleString() || '0'}
          </div>
          <div style={{ fontSize: '13px', color: '#6c757d', marginTop: 4 }}>
            Across {summary?.channels.length || 0} channels
          </div>
        </div>

        <div style={card}>
          <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Spend by Channel</h3>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={chartData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis type="number" tickFormatter={(v) => `$${v >= 1000 ? `${(v / 1000).toFixed(0)}K` : v}`} />
              <YAxis type="category" dataKey="channel" width={110} tick={{ fontSize: 12 }} />
              <Tooltip formatter={(value: number) => `$${value.toLocaleString()}`} />
              <Bar dataKey="amount" fill="#dc3545" name="Spend" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Add Expense Form */}
      <div style={{ ...card, marginBottom: 24 }}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>Add Expense</h3>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 2fr auto', gap: 12, alignItems: 'end' }}>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Channel</label>
            <select
              value={newChannel}
              onChange={(e) => setNewChannel(e.target.value)}
              style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6 }}
            >
              {CHANNEL_OPTIONS.map(ch => (
                <option key={ch} value={ch}>{ch.replace(/_/g, ' ')}</option>
              ))}
            </select>
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Amount ($)</label>
            <input
              type="number"
              value={newAmount}
              onChange={(e) => setNewAmount(e.target.value)}
              placeholder="0.00"
              style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Period</label>
            <input
              type="month"
              value={newPeriod}
              onChange={(e) => setNewPeriod(e.target.value)}
              style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
            />
          </div>
          <div>
            <label style={{ display: 'block', fontSize: '12px', fontWeight: '600', color: '#6c757d', marginBottom: 4 }}>Notes</label>
            <input
              type="text"
              value={newNotes}
              onChange={(e) => setNewNotes(e.target.value)}
              placeholder="Optional notes"
              style={{ width: '100%', padding: '10px', fontSize: '14px', border: '1px solid #dee2e6', borderRadius: 6, boxSizing: 'border-box' }}
            />
          </div>
          <button
            onClick={handleAdd}
            disabled={addMutation.isPending || !newAmount}
            style={{
              padding: '10px 24px', fontSize: '14px', fontWeight: '600',
              backgroundColor: (!newAmount || addMutation.isPending) ? '#ccc' : '#28a745',
              color: 'white', border: 'none', borderRadius: 6,
              cursor: (!newAmount || addMutation.isPending) ? 'not-allowed' : 'pointer',
              whiteSpace: 'nowrap',
            }}
          >
            {addMutation.isPending ? 'Adding...' : 'Add'}
          </button>
        </div>
      </div>

      {/* Expense List */}
      <div style={card}>
        <h3 style={{ margin: '0 0 16px', fontSize: '16px', fontWeight: '600', color: '#495057' }}>All Expenses</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
            <thead>
              <tr style={{ backgroundColor: '#f8f9fa' }}>
                {['Channel', 'Amount', 'Period', 'Notes', 'Actions'].map(h => (
                  <th key={h} style={{ padding: '12px 16px', textAlign: h === 'Actions' ? 'center' : h === 'Amount' ? 'right' : 'left', fontWeight: '600', color: '#495057', borderBottom: '2px solid #e9ecef' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {expenses.map((exp, idx) => (
                <tr key={exp.id} style={{ borderBottom: '1px solid #f1f3f5', backgroundColor: idx % 2 === 0 ? '#fff' : '#f8f9fa' }}>
                  <td style={{ padding: '10px 16px' }}>
                    <span style={{
                      display: 'inline-block', padding: '2px 10px', borderRadius: 4,
                      fontSize: '12px', fontWeight: '600',
                      backgroundColor: `${COLORS[exp.channel] || '#6c757d'}20`,
                      color: COLORS[exp.channel] || '#6c757d',
                    }}>
                      {exp.channel.replace(/_/g, ' ')}
                    </span>
                  </td>
                  <td style={{ padding: '10px 16px', textAlign: 'right', fontWeight: '600', color: '#dc3545' }}>
                    ${exp.amount.toLocaleString()}
                  </td>
                  <td style={{ padding: '10px 16px', color: '#6c757d' }}>{exp.period || '-'}</td>
                  <td style={{ padding: '10px 16px', color: '#6c757d', fontSize: '13px' }}>{exp.notes || '-'}</td>
                  <td style={{ padding: '10px 16px', textAlign: 'center' }}>
                    <button
                      onClick={() => deleteMutation.mutate(exp.id)}
                      style={{
                        padding: '4px 12px', fontSize: '12px', fontWeight: '600',
                        backgroundColor: '#fff', color: '#dc3545',
                        border: '1px solid #dc3545', borderRadius: 4, cursor: 'pointer',
                      }}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
              {expenses.length === 0 && (
                <tr>
                  <td colSpan={5} style={{ padding: 24, textAlign: 'center', color: '#6c757d' }}>
                    No expenses yet. Add one above or connect an ad platform to auto-import.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
