import type { Dispatch, SetStateAction } from 'react'

import type {
  MeiroConfig,
  MeiroImportResult,
  MeiroMappingState,
  MeiroPullConfig,
  MeiroQuarantineReprocessResult,
  MeiroQuarantineRun,
  MeiroWebhookEvent,
  MeiroWebhookDiagnostics,
  MeiroWebhookSuggestions,
} from '../../connectors/meiroConnector'
import { tokens as t } from '../../theme/tokens'
import MeiroCdpSettings from './MeiroCdpSettings'
import MeiroImportReplay from './MeiroImportReplay'
import MeiroNormalization from './MeiroNormalization'
import MeiroOverview from './MeiroOverview'
import MeiroPipesSettings from './MeiroPipesSettings'
import type { DryRunResult, MeiroTab, MeiroWebhookArchiveStatus } from './shared'
import type { MeiroWebhookReprocessResult } from './shared'

interface MeiroIntegrationPanelProps {
  meiroTab: MeiroTab
  setMeiroTab: (tab: MeiroTab) => void
  meiroUrl: string
  setMeiroUrl: Dispatch<SetStateAction<string>>
  meiroKey: string
  setMeiroKey: Dispatch<SetStateAction<string>>
  webhookSecretValue: string | null
  meiroPullDraft: MeiroPullConfig
  setMeiroPullDraft: Dispatch<SetStateAction<MeiroPullConfig>>
  meiroConfig?: MeiroConfig
  meiroMappingState?: MeiroMappingState
  meiroWebhookSuggestions?: MeiroWebhookSuggestions
  meiroWebhookEvents?: { items: MeiroWebhookEvent[]; total: number }
  meiroWebhookDiagnostics?: MeiroWebhookDiagnostics
  meiroWebhookArchiveStatus?: MeiroWebhookArchiveStatus
  meiroEventArchiveStatus?: MeiroWebhookArchiveStatus
  meiroWebhookEventsLoading: boolean
  meiroWebhookEventsError?: string | null
  meiroWebhookDiagnosticsError?: string | null
  meiroWebhookSuggestionsLoading: boolean
  meiroWebhookSuggestionsError?: string | null
  testMeiroResult?: { message?: string } | null
  saveMeiroPullPending: boolean
  runMeiroPullPending: boolean
  applyMeiroMappingSuggestionPending: boolean
  updateMeiroMappingApprovalPending: boolean
  meiroDryRunPending: boolean
  meiroDryRunData?: DryRunResult
  importFromMeiroPending: boolean
  importFromMeiroResult?: MeiroImportResult | null
  reprocessWebhookArchivePending: boolean
  reprocessWebhookArchiveResult?: MeiroWebhookReprocessResult | null
  reprocessQuarantinePending?: boolean
  reprocessQuarantineResult?: MeiroQuarantineReprocessResult | null
  reprocessQuarantineError?: string | null
  quarantineRuns?: { items: MeiroQuarantineRun[]; total: number }
  quarantineRunsLoading?: boolean
  quarantineRunsError?: string | null
  selectedQuarantineRun?: MeiroQuarantineRun | null
  selectedQuarantineRunLoading?: boolean
  selectedQuarantineRunError?: string | null
  relativeTime: (iso?: string | null) => string
  setOauthToast: Dispatch<SetStateAction<string | null>>
  onTestMeiro: () => void
  onConnectMeiro: () => void
  onDisconnectMeiro: () => void
  onRotateWebhookSecret: () => void
  onSaveMeiroPull: () => void
  onRunMeiroPull: () => void
  onSaveMeiroMapping: (payload: Record<string, unknown>) => void
  onApplyMeiroMappingSuggestion: () => void
  onApproveMeiroMapping: () => void
  onRejectMeiroMapping: () => void
  onDryRun: () => void
  onImportFromMeiro: () => void
  onReplayArchive: () => void
  onReprocessSelectedQuarantine?: (recordIndices?: number[]) => void
  onSelectQuarantineRun: (runId: string) => void
}

export default function MeiroIntegrationPanel(props: MeiroIntegrationPanelProps) {
  const { meiroTab, setMeiroTab } = props

  return (
    <>
      <div
        style={{
          display: 'inline-flex',
          gap: t.space.xs,
          flexWrap: 'wrap',
          padding: t.space.xs,
          border: `1px solid ${t.color.borderLight}`,
          borderRadius: t.radius.md,
          background: t.color.bgSubtle,
        }}
      >
        {(
          [
            { id: 'overview', label: 'Overview' },
            { id: 'cdp', label: 'CDP Pull' },
            { id: 'pipes', label: 'Pipes Webhook' },
            { id: 'normalization', label: 'Normalization' },
            { id: 'import', label: 'Import & Replay' },
          ] as const
        ).map((tab) => (
          <button
            key={tab.id}
            type="button"
            onClick={() => setMeiroTab(tab.id)}
            style={{
              border: `1px solid ${meiroTab === tab.id ? t.color.accent : 'transparent'}`,
              background: meiroTab === tab.id ? t.color.accentMuted : 'transparent',
              color: meiroTab === tab.id ? t.color.accent : t.color.textSecondary,
              borderRadius: t.radius.sm,
              padding: '6px 12px',
              cursor: 'pointer',
              fontSize: t.font.sizeSm,
              fontWeight: meiroTab === tab.id ? t.font.weightSemibold : t.font.weightMedium,
            }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {meiroTab === 'overview' && (
        <MeiroOverview
          meiroConfig={props.meiroConfig}
          meiroMappingState={props.meiroMappingState}
          meiroWebhookArchiveStatus={props.meiroWebhookArchiveStatus}
          runMeiroPullPending={props.runMeiroPullPending}
          importFromMeiroPending={props.importFromMeiroPending}
          relativeTime={props.relativeTime}
          setMeiroTab={setMeiroTab}
          onTestMeiro={props.onTestMeiro}
          onRunMeiroPull={props.onRunMeiroPull}
          onImportFromMeiro={props.onImportFromMeiro}
        />
      )}

      {meiroTab === 'cdp' && (
        <MeiroCdpSettings
          meiroUrl={props.meiroUrl}
          setMeiroUrl={props.setMeiroUrl}
          meiroKey={props.meiroKey}
          setMeiroKey={props.setMeiroKey}
          meiroPullDraft={props.meiroPullDraft}
          setMeiroPullDraft={props.setMeiroPullDraft}
          meiroConfig={props.meiroConfig}
          meiroWebhookSuggestions={props.meiroWebhookSuggestions}
          meiroWebhookSuggestionsLoading={props.meiroWebhookSuggestionsLoading}
          meiroWebhookSuggestionsError={props.meiroWebhookSuggestionsError}
          testMeiroResult={props.testMeiroResult}
          saveMeiroPullPending={props.saveMeiroPullPending}
          runMeiroPullPending={props.runMeiroPullPending}
          relativeTime={props.relativeTime}
          onTestMeiro={props.onTestMeiro}
          onConnectMeiro={props.onConnectMeiro}
          onDisconnectMeiro={props.onDisconnectMeiro}
          onSaveMeiroPull={props.onSaveMeiroPull}
          onRunMeiroPull={props.onRunMeiroPull}
        />
      )}

      {meiroTab === 'pipes' && (
        <MeiroPipesSettings
          meiroConfig={props.meiroConfig}
          meiroPullDraft={props.meiroPullDraft}
          setMeiroPullDraft={props.setMeiroPullDraft}
          webhookSecretValue={props.webhookSecretValue}
          meiroWebhookEvents={props.meiroWebhookEvents}
          meiroWebhookDiagnostics={props.meiroWebhookDiagnostics}
          meiroWebhookArchiveStatus={props.meiroWebhookArchiveStatus}
          meiroEventArchiveStatus={props.meiroEventArchiveStatus}
          meiroWebhookEventsLoading={props.meiroWebhookEventsLoading}
          meiroWebhookEventsError={props.meiroWebhookEventsError}
          meiroWebhookDiagnosticsError={props.meiroWebhookDiagnosticsError}
          relativeTime={props.relativeTime}
          setOauthToast={props.setOauthToast}
          onRotateWebhookSecret={props.onRotateWebhookSecret}
          onSaveMeiroPull={props.onSaveMeiroPull}
          saveMeiroPullPending={props.saveMeiroPullPending}
        />
      )}

      {meiroTab === 'normalization' && (
        <MeiroNormalization
          meiroMappingState={props.meiroMappingState}
          meiroWebhookSuggestions={props.meiroWebhookSuggestions}
          applyMeiroMappingSuggestionPending={props.applyMeiroMappingSuggestionPending}
          updateMeiroMappingApprovalPending={props.updateMeiroMappingApprovalPending}
          relativeTime={props.relativeTime}
          onSaveMeiroMapping={props.onSaveMeiroMapping}
          onApplyMeiroMappingSuggestion={props.onApplyMeiroMappingSuggestion}
          onApproveMeiroMapping={props.onApproveMeiroMapping}
          onRejectMeiroMapping={props.onRejectMeiroMapping}
        />
      )}

      {meiroTab === 'import' && (
        <MeiroImportReplay
          meiroConfig={props.meiroConfig}
          meiroPullDraft={props.meiroPullDraft}
          meiroMappingState={props.meiroMappingState}
          meiroWebhookArchiveStatus={props.meiroWebhookArchiveStatus}
          meiroEventArchiveStatus={props.meiroEventArchiveStatus}
          meiroWebhookSuggestions={props.meiroWebhookSuggestions}
          meiroDryRunPending={props.meiroDryRunPending}
          meiroDryRunData={props.meiroDryRunData}
          importFromMeiroPending={props.importFromMeiroPending}
          importFromMeiroResult={props.importFromMeiroResult}
          reprocessWebhookArchivePending={props.reprocessWebhookArchivePending}
          reprocessWebhookArchiveResult={props.reprocessWebhookArchiveResult}
          reprocessQuarantinePending={props.reprocessQuarantinePending || false}
          reprocessQuarantineResult={props.reprocessQuarantineResult ?? null}
          reprocessQuarantineError={props.reprocessQuarantineError}
          quarantineRuns={props.quarantineRuns}
          quarantineRunsLoading={props.quarantineRunsLoading || false}
          quarantineRunsError={props.quarantineRunsError}
          selectedQuarantineRun={props.selectedQuarantineRun}
          selectedQuarantineRunLoading={props.selectedQuarantineRunLoading || false}
          selectedQuarantineRunError={props.selectedQuarantineRunError}
          relativeTime={props.relativeTime}
          onDryRun={props.onDryRun}
          onImportFromMeiro={props.onImportFromMeiro}
          onReplayArchive={props.onReplayArchive}
          onReprocessSelectedQuarantine={props.onReprocessSelectedQuarantine || (() => {})}
          onSelectQuarantineRun={props.onSelectQuarantineRun}
        />
      )}
    </>
  )
}
