param(
    [string]$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "../../..")),
    [string]$OutputPath = (Join-Path $PSScriptRoot "../phase1/claims.csv")
)

$ErrorActionPreference = "Stop"

function Normalize-Text([string]$Text) {
    $value = $Text -replace '<!--.*?-->', ''
    $value = $value -replace '^\s*[-*]\s+', ''
    $value = $value -replace '\*\*|__|`', ''
    $value = $value.Trim('*')
    $value = $value -replace '\s+', ' '
    return $value.Trim()
}

function Get-StableId([string]$Text) {
    $bytes = [Text.Encoding]::UTF8.GetBytes($Text.ToLowerInvariant())
    $hash = [Security.Cryptography.SHA256]::Create().ComputeHash($bytes)
    $hex = -join ($hash | ForEach-Object { $_.ToString('x2') })
    return "CQL-WP-$($hex.Substring(0, 12).ToUpperInvariant())"
}

function Get-Domain([string]$Section, [string]$Claim) {
    $value = "$Section $Claim"
    if ($value -match '(?i)security|privilege|GDPR|SOX|HIPAA|tenant|audit|lineage|classification') { return 'security-governance' }
    if ($value -match '(?i)MCP|REMOTE|federat|provider|identity resolution|namespace') { return 'federation-identity' }
    if ($value -match '(?i)process|event log|trace|conform|activity|variant|throughput|rework') { return 'process-intelligence' }
    if ($value -match '(?i)bitmap|Arrow|Parquet|storage|MVCC|cache|snapshot|refresh|incremental|stream') { return 'storage-lifecycle' }
    if ($value -match '(?i)CLI|REPL|Python|Jupyter|LLM|language server|VS Code|SDK|REST|gRPC|JDBC|ODBC|diagnostic|error') { return 'developer-platform' }
    if ($value -match '(?i)optimizer|physical operator|pipeline|pushdown|cost|execution|adapter|DuckDB|Polars') { return 'execution-optimization' }
    if ($value -match '(?i)score|rank|window|algebra|CONTEXT IN|CONTEXT ON|THEN|type|NULL|coercion|DDL|syntax|grammar') { return 'language-semantics' }
    return 'product-architecture'
}

function Get-ClaimClass([string]$Section, [string]$Claim) {
    $value = "$Section $Claim"
    if ($Section -match '(?i)Implementation Status|Implementation Strategy|Future Directions' -or $Claim -match '(?i)implemented|specified|designed|deferred|planned|roadmap') { return 'maturity-status' }
    if ($Claim -match '(?i)\bmust\b|\bshall\b|required|prohibited|never|only if|privilege') { return 'requirement' }
    if ($value -match '(?i)syntax|DDL|statement|clause|operator precedence|literal|comment') { return 'language-surface' }
    if ($value -match '(?i)performance|latency|millisecond|throughput|complexity|O\(') { return 'performance-target' }
    if ($value -match '(?i)API|SDK|CLI|REPL|REST|gRPC|JDBC|ODBC|LSP|language server') { return 'interface-contract' }
    if ($value -match '(?i)architecture|layer|pipeline|stage|operator|storage|tier|module') { return 'architecture' }
    if ($value -match '(?i)semantics|returns|resolves|evaluat|score|membership|default|behavio') { return 'behavior-semantics' }
    return 'design-claim'
}

function Get-Maturity([string]$Section, [string]$Claim) {
    $value = "$Section $Claim"
    if ($Section -match '(?i)Implemented') { return 'implemented (whitepaper v0.2 assertion)' }
    if ($Section -match '(?i)Specified \(Reference Architecture\)') { return 'specified reference architecture' }
    if ($Section -match '(?i)Designed \(Protocol Surface\)') { return 'designed protocol surface' }
    if ($Section -match '(?i)Deferred to v2') { return 'deferred to v2' }
    if ($Section -match '(?i)Future Directions') { return 'future direction; version unspecified' }
    if ($value -match '(?i)\bv2\b') { return 'v2 target or discussion' }
    if ($value -match '(?i)\bv1\b') { return 'v1 target or contract' }
    if ($value -match '(?i)Phase 1') { return 'implementation phase 1' }
    if ($value -match '(?i)Phase 2') { return 'implementation phase 2' }
    return 'architecture claim; maturity not stated locally'
}

function Get-Normativity([string]$Claim) {
    if ($Claim -match '(?i)\bmust\b|\bshall\b|required|prohibited|never|guarantee') { return 'normative' }
    if ($Claim -match '(?i)\bshould\b|recommended|target|expected') { return 'aspirational' }
    if ($Claim -match '(?i)proposes|designed|future|could|may ') { return 'design-intent' }
    return 'descriptive assertion'
}

function Get-Evidence([string]$Domain, [string]$Class, [string]$Claim) {
    if ($Class -eq 'maturity-status') { return 'implementation artifact plus executable acceptance test or release record' }
    if ($Class -eq 'performance-target') { return 'reproducible benchmark with dataset, hardware, configuration, and raw results' }
    if ($Domain -eq 'language-semantics') { return 'grammar/parser support, semantic implementation, and positive/negative conformance tests' }
    if ($Domain -eq 'execution-optimization') { return 'execution implementation, plan inspection, correctness tests, and relevant benchmark' }
    if ($Domain -eq 'storage-lifecycle') { return 'storage/catalog implementation, transition and concurrency tests, and recovery evidence' }
    if ($Domain -eq 'federation-identity') { return 'provider/transport implementation, contract tests, failure-mode tests, and integration fixture' }
    if ($Domain -eq 'process-intelligence') { return 'function/DDL implementation and event-log conformance fixtures' }
    if ($Domain -eq 'security-governance') { return 'enforcement implementation, adversarial tests, audit evidence, and documented threat/control mapping' }
    if ($Domain -eq 'developer-platform') { return 'public interface implementation, protocol/API tests, and user-facing example' }
    return 'normative specification or decision plus implementation and acceptance test where claimed as shipped'
}

function Get-CrossReferences([string]$Claim) {
    $refs = New-Object System.Collections.Generic.List[string]
    if ($Claim -match '(?i)score|WEIGHTED|intersection') { $refs.Add('SPEC.md:246-261; DECISIONS.md:53-80,604-645') }
    if ($Claim -match '(?i)THEN') { $refs.Add('SPEC.md:132-138; DECISIONS.md:110-123') }
    if ($Claim -match '(?i)TEMPORAL|event-time') { $refs.Add('SPEC.md:250-257,327-334; DECISIONS.md:147-154,1332-1344,1396-1403') }
    if ($Claim -match '(?i)snapshot|bitmap|membership|Roaring') { $refs.Add('SPEC.md:306-325; DECISIONS.md:1107-1403; docs/architecture/BITMAP_CONTEXT_STORAGE.md') }
    if ($Claim -match '(?i)MCP|REMOTE|provider|federat') { $refs.Add('SPEC.md:140-148,464-491; DECISIONS.md:198-294,549-672; docs/architecture/DEEPSEE_CONNECTOR.md') }
    if ($Claim -match '(?i)event log|process model|process intelligence|throughput|rework|variant|conform') { $refs.Add('SPEC.md:40-50,175-186,422-462; DECISIONS.md:296-406') }
    if ($Claim -match '(?i)error code|diagnostic|W100') { $refs.Add('SPEC.md:528-551; DECISIONS.md:945-955') }
    if ($Claim -match '(?i)language server|LSP') { $refs.Add('docs/LANGUAGE_SERVER_SPEC.md:24-158; README.md:318-326') }
    if ($Claim -match '(?i)Python SDK|CLI|REPL|Jupyter') { $refs.Add('README.md:102-263,303-326; docs/TOOLING.md:25-209') }
    if ($Claim -match '(?i)security|privilege|GDPR|SOX|HIPAA|tenant|audit') { $refs.Add('SPEC.md:493-514; DECISIONS.md:700-796') }
    return ($refs -join '; ')
}

function Get-Conflict([int]$StartLine, [string]$Claim) {
    $items = New-Object System.Collections.Generic.List[string]
    if ($StartLine -ge 112 -and $StartLine -le 128) { $items.Add('Implementation-status assertion requires code/test verification; README.md:54-74 gives a narrower shipped-versus-designed boundary.') }
    if ($Claim -match '(?i)27 statement types') { $items.Add('Count is version-sensitive and should be generated from grammar/tests rather than maintained manually.') }
    if ($Claim -match '(?i)linter with 11 rules') { $items.Add('README.md:454-472 currently enumerates a different/version-sensitive public lint surface; verify registry rather than prose count.') }
    if ($Claim -match '(?i)W100') { $items.Add('Warning-code collision: SPEC.md:323,547 and contextql/errors.py:158-163 assign W100 to stale snapshots, while score range uses W003 in contextql/errors.py:170-171.') }
    if ($Claim -match '(?i)W101.*CONTEXT WINDOW|CONTEXT WINDOW.*W101') { $items.Add('Warning-code collision: SPEC.md:325,547 assigns W101 to failed refresh; contextql/errors.py:165-167 assigns window-without-score to W001.') }
    if ($Claim -match '(?i)W010|W012') { $items.Add('These freshness warning codes are absent from SPEC.md and contextql/errors.py; the current contract uses W100 for stale and W101 for failed refresh.') }
    if ($Claim -match '(?i)W013') { $items.Add('W013 exists as a decision in DECISIONS.md:410-419 but is absent from SPEC.md and the implementation registry.') }
    if ($Claim -match '(?i)O\(1\).*membership') { $items.Add('Asymptotic statement is stronger than the architecture documents, which describe bitmap operations but do not establish end-to-end O(1) retrieval.') }
    if ($Claim -match '(?i)nine states|9 states') { $items.Add('SPEC.md:400-420 defines lifecycle statements but does not canonically enumerate the whitepaper nine-state machine.') }
    if ($Claim -match '(?i)millisecond-class') { $items.Add('Requires benchmark provenance; docs/plans/credibility-correctness-consolidation.md:136-154 treats provenance as unfinished work.') }
    if ($Claim -match '(?i)(streaming context|streaming connector|Kafka|Flink|micro-batch)') { $items.Add('Whitepaper status defers streaming integration to v2; DECISIONS.md:431-439 also marks it deferred.') }
    if ($Claim -match '(?i)OCEL') { $items.Add('Whitepaper status defers OCEL to v2; DECISIONS.md:390-397 confirms deferral.') }
    if ($Claim -match '(?i)LLM-driven context synthesis') { $items.Add('Whitepaper status defers synthesis to v2 while Section 34 describes a substantial API surface; maturity labeling must remain explicit.') }
    if ($Claim -match '(?i)standard SQL queries pass through') { $items.Add('Needs dialect-by-dialect conformance evidence; Section 40 explicitly limits SQL conformance.') }
    if ($Claim -match '(?i)score.*\[0\.0, 1\.0\]|scores outside') { $items.Add('SPEC.md:246-249 recommends [0,1], and DECISIONS.md:70-78 requires a warning outside it; the implementation assigns that warning W003, not whitepaper W100.') }
    if ($Claim -match '(?i)Temporal filters operate on temporal column values|AT qualifier filters by the temporal column') { $items.Add('Direct semantic conflict: SPEC.md:327-344 and DECISIONS.md:1332-1344 define AT/BETWEEN against recorded membership history, superseding the older temporal-column filtering model in DECISIONS.md:147-152.') }
    if ($Claim -match '(?i)CONTEXT_SCORE\(\).*E111|Both are valid only.*E111') { $items.Add('Error-code conflict: README.md:465 and contextql/errors.py:62-69 assign scope violations to E108; E111 is score-expression type error.') }
    if ($Claim -match '(?i)9-state|nine-state') { $items.Add('The executable catalog currently stores free-form string states (including active/draft) rather than a canonical nine-state type; SPEC.md does not enumerate this state machine.') }
    if ($Claim -match '(?i)DuckDB, Polars, and Arrow') { $items.Add('README.md:54-74 identifies a hybrid DuckDB engine and DuckDB adapter; equivalent Polars/Arrow execution support is not asserted there.') }
    return ($items -join ' ')
}

$whitepaperPath = Join-Path $RepositoryRoot 'WHITEPAPER.md'
$lines = Get-Content -LiteralPath $whitepaperPath -Encoding UTF8
$records = New-Object System.Collections.Generic.List[object]
$section = 'Document preface'
$inCode = $false
$paragraph = New-Object System.Collections.Generic.List[string]
$paragraphStart = 0
$tableHeaders = $null

function Add-ParagraphClaims {
    param([System.Collections.Generic.List[string]]$Buffer, [int]$Start, [int]$End, [string]$CurrentSection)
    if ($Buffer.Count -eq 0) { return }
    $raw = ($Buffer -join ' ')
    $text = Normalize-Text $raw
    $Buffer.Clear()
    if ($CurrentSection -in @('Document preface', 'Table of Contents')) { return }
    if ($text.Length -lt 35 -or $text.EndsWith('?') -or $text -match '^\*?Figure \d' -or $text -match '^Table of Contents') { return }

    # Sentences are the extraction unit. Semicolon-delimited independent clauses are
    # split as well; commas and conjunctions are deliberately preserved because
    # blindly splitting them changes technical semantics.
    $sentences = [regex]::Split($text, '(?<=[.!?])\s+(?=[A-Z`(])|\s*;\s+(?=[A-Z`])')
    foreach ($sentenceRaw in $sentences) {
        $claim = Normalize-Text $sentenceRaw
        if ($claim.Length -lt 28 -or $claim.EndsWith('?')) { continue }
        if ($claim -match '^(For example|Consider the|Organizations increasingly require|The following (table|query|example)|This section)') { continue }
        $domain = Get-Domain $CurrentSection $claim
        $class = Get-ClaimClass $CurrentSection $claim
        $conflict = Get-Conflict $Start $claim
        $risk = if ($conflict) { 'high' } elseif ($domain -in @('security-governance','storage-lifecycle','federation-identity') -or $class -eq 'performance-target') { 'medium' } else { 'normal' }
        $notes = if ($claim -match '\band\b|,') { 'Source sentence retained as one review unit; reviewer should split further if the linked clauses can vary independently.' } else { 'Direct declarative extraction from the whitepaper.' }
        $records.Add([pscustomobject][ordered]@{
            claim_id = Get-StableId $claim
            source_path = 'WHITEPAPER.md'
            source_lines = if ($Start -eq $End) { "$Start" } else { "$Start-$End" }
            section = $CurrentSection
            atomic_claim = $claim
            claim_class = $class
            domain = $domain
            stated_maturity_or_target_version = Get-Maturity $CurrentSection $claim
            normativity = Get-Normativity $claim
            expected_evidence = Get-Evidence $domain $class $claim
            corroborating_or_related_sources = Get-CrossReferences $claim
            potential_conflict_or_drift = $conflict
            review_risk = $risk
            initial_notes = $notes
        })
    }
}

for ($index = 0; $index -lt $lines.Count; $index++) {
    $lineNumber = $index + 1
    $line = $lines[$index]
    if ($line -match '^```') {
        Add-ParagraphClaims $paragraph $paragraphStart ($lineNumber - 1) $section
        $inCode = -not $inCode
        continue
    }
    if ($inCode) { continue }
    if ($line -match '^(#{1,3})\s+(.+)$') {
        Add-ParagraphClaims $paragraph $paragraphStart ($lineNumber - 1) $section
        $heading = Normalize-Text $Matches[2]
        if ($heading -notmatch '^(ContextQL|A Context-Native Query Language)') { $section = $heading }
        $tableHeaders = $null
        continue
    }
    if ($line -match '^\s*\|') {
        Add-ParagraphClaims $paragraph $paragraphStart ($lineNumber - 1) $section
        $cells = @($line.Trim().Trim('|').Split('|') | ForEach-Object { Normalize-Text $_ })
        $isSeparator = $true
        foreach ($cell in $cells) { if ($cell -notmatch '^:?-{3,}:?$') { $isSeparator = $false; break } }
        $nextIsSeparator = $false
        if ($index + 1 -lt $lines.Count -and $lines[$index + 1] -match '^\s*\|') {
            $nextCells = @($lines[$index + 1].Trim().Trim('|').Split('|') | ForEach-Object { $_.Trim() })
            $nextIsSeparator = $true
            foreach ($cell in $nextCells) { if ($cell -notmatch '^:?-{3,}:?$') { $nextIsSeparator = $false; break } }
        }
        if ($nextIsSeparator) { $tableHeaders = $cells; continue }
        if ($isSeparator) { continue }
        if ($null -ne $tableHeaders -and $cells.Count -eq $tableHeaders.Count) {
            $parts = New-Object System.Collections.Generic.List[string]
            for ($cellIndex = 1; $cellIndex -lt $cells.Count; $cellIndex++) {
                if ($cells[$cellIndex]) { $parts.Add("$($tableHeaders[$cellIndex]) = $($cells[$cellIndex])") }
            }
            if ($cells[0] -and $parts.Count -gt 0) {
                $claim = "$($tableHeaders[0]) '$($cells[0])': $($parts -join '; ')."
                $domain = Get-Domain $section $claim
                $class = Get-ClaimClass $section $claim
                $conflict = Get-Conflict $lineNumber $claim
                $risk = if ($conflict) { 'high' } elseif ($domain -in @('security-governance','storage-lifecycle','federation-identity') -or $class -eq 'performance-target') { 'medium' } else { 'normal' }
                $records.Add([pscustomobject][ordered]@{
                    claim_id = Get-StableId $claim
                    source_path = 'WHITEPAPER.md'
                    source_lines = "$lineNumber"
                    section = $section
                    atomic_claim = $claim
                    claim_class = $class
                    domain = $domain
                    stated_maturity_or_target_version = Get-Maturity $section $claim
                    normativity = Get-Normativity $claim
                    expected_evidence = Get-Evidence $domain $class $claim
                    corroborating_or_related_sources = Get-CrossReferences $claim
                    potential_conflict_or_drift = $conflict
                    review_risk = $risk
                    initial_notes = 'Normalized from one Markdown table row; split if individual cells can vary independently.'
                })
            }
        }
        continue
    }
    if ([string]::IsNullOrWhiteSpace($line) -or $line -match '^\s*---\s*$') {
        Add-ParagraphClaims $paragraph $paragraphStart ($lineNumber - 1) $section
        $tableHeaders = $null
        continue
    }
    if ($line -match '^\s*(?:[-*]|\d+\.)\s+') {
        Add-ParagraphClaims $paragraph $paragraphStart ($lineNumber - 1) $section
        $paragraphStart = $lineNumber
        $paragraph.Add($line)
        Add-ParagraphClaims $paragraph $paragraphStart $lineNumber $section
        continue
    }
    if ($paragraph.Count -eq 0) { $paragraphStart = $lineNumber }
    $paragraph.Add($line)
}
Add-ParagraphClaims $paragraph $paragraphStart $lines.Count $section

# Chapter 39 is intentionally a compact, code-only DDL reference. Preserve each
# semicolon-terminated form as a language-surface claim instead of discarding the
# fenced block as an example. Other code blocks remain excluded because they are
# predominantly illustrative queries rather than prose contracts.
$ddlSection = $null
$ddlInCode = $false
$ddlBuffer = New-Object System.Collections.Generic.List[string]
$ddlStart = 0
for ($index = 0; $index -lt $lines.Count; $index++) {
    $lineNumber = $index + 1
    $line = $lines[$index]
    if ($line -match '^##\s+(39\..+)$') { $ddlSection = Normalize-Text $Matches[1]; continue }
    if ($line -match '^#\s+' -and $line -notmatch '^##') { if ($lineNumber -gt 2442) { $ddlSection = $null }; continue }
    if ($null -eq $ddlSection) { continue }
    if ($line -match '^```') { $ddlInCode = -not $ddlInCode; continue }
    if (-not $ddlInCode) { continue }
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith('--')) { continue }
    if ($ddlBuffer.Count -eq 0) { $ddlStart = $lineNumber }
    $ddlBuffer.Add($trimmed)
    if (-not $trimmed.EndsWith(';')) { continue }
    $syntax = ($ddlBuffer -join ' ') -replace '\s+', ' '
    $ddlBuffer.Clear()
    $claim = "Supported DDL form: $syntax"
    $records.Add([pscustomobject][ordered]@{
        claim_id = Get-StableId $claim
        source_path = 'WHITEPAPER.md'
        source_lines = if ($ddlStart -eq $lineNumber) { "$lineNumber" } else { "$ddlStart-$lineNumber" }
        section = $ddlSection
        atomic_claim = $claim
        claim_class = 'language-surface'
        domain = 'language-semantics'
        stated_maturity_or_target_version = 'architecture claim; maturity not stated locally'
        normativity = 'descriptive assertion'
        expected_evidence = 'grammar/parser support, semantic implementation, and positive/negative conformance tests'
        corroborating_or_related_sources = 'SPEC.md:24-61,221-261,375-491'
        potential_conflict_or_drift = ''
        review_risk = 'normal'
        initial_notes = 'Normalized from the code-only DDL reference; optional bracketed clauses are preserved verbatim.'
    })
}

# Exact duplicate prose can recur in reference sections. Keep the first occurrence
# so claim IDs remain unique; cross-section restatements remain discoverable by rg.
$unique = $records | Group-Object claim_id | ForEach-Object { $_.Group[0] } | Sort-Object { [int](($_.source_lines -split '-')[0]) }, claim_id
$outputDirectory = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
$unique | Export-Csv -LiteralPath $OutputPath -NoTypeInformation -Encoding UTF8
Write-Output "Wrote $($unique.Count) whitepaper claims to $OutputPath"
