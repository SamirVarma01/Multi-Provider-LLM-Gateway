package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/ledger"
	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/obs"
)

const (
	defaultLedgerPage = 100
	maxLedgerPage     = 1000
)

// usageResponse reports amounts in USD to read and nano-USD to compare
// exactly. Budget, total spent, held, and available are current; the rest
// cover the requested window.
type usageResponse struct {
	KeyID          uuid.UUID    `json:"key_id"`
	From           *time.Time   `json:"from"`
	To             *time.Time   `json:"to"`
	BudgetUSD      *float64     `json:"budget_usd"`
	BudgetNUSD     *int64       `json:"budget_nusd"`
	TotalSpentUSD  float64      `json:"total_spent_usd"`
	TotalSpentNUSD int64        `json:"total_spent_nusd"`
	HeldUSD        float64      `json:"held_usd"`
	HeldNUSD       int64        `json:"held_nusd"`
	AvailableUSD   *float64     `json:"available_usd"`
	AvailableNUSD  *int64       `json:"available_nusd"`
	SpentUSD       float64      `json:"spent_usd"`
	SpentNUSD      int64        `json:"spent_nusd"`
	Requests       int64        `json:"requests"`
	Groups         []usageGroup `json:"groups"`
}

type usageGroup struct {
	Alias            string  `json:"alias"`
	Provider         string  `json:"provider"`
	UpstreamModel    string  `json:"upstream_model"`
	Requests         int64   `json:"requests"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	SpentUSD         float64 `json:"spent_usd"`
	SpentNUSD        int64   `json:"spent_nusd"`
}

func usd(nusd int64) float64 { return float64(nusd) / 1e9 }

func usdPtr(nusd *int64) *float64 {
	if nusd == nil {
		return nil
	}
	v := usd(*nusd)
	return &v
}

func timeParam(r *http.Request, name string) (*time.Time, *apiError) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, badRequest("invalid_time", "%s must be an RFC 3339 time.", name)
	}
	return &t, nil
}

// reportFailed answers a failed read of a key's ledger.
func reportFailed(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, ledger.ErrKeyNotFound) {
		oai.WriteError(w, r, http.StatusNotFound, oai.ErrTypeNotFound, "key_not_found", "No such key.")
		return
	}
	obs.Logger(r.Context()).Error("ledger read failed", "err", err)
	oai.WriteError(w, r, http.StatusInternalServerError, oai.ErrTypeAPI, "internal_error", "Could not read the ledger.")
}

func (s *Server) handleKeyUsage(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		badRequest("invalid_id", "Invalid key id.").write(w, r)
		return
	}
	from, aerr := timeParam(r, "from")
	if aerr != nil {
		aerr.write(w, r)
		return
	}
	to, aerr := timeParam(r, "to")
	if aerr != nil {
		aerr.write(w, r)
		return
	}
	u, err := s.opts.Reports.Usage(r.Context(), id, from, to)
	if err != nil {
		reportFailed(w, r, err)
		return
	}
	available := u.AvailableNUSD()
	out := usageResponse{
		KeyID: id, From: from, To: to,
		BudgetUSD: usdPtr(u.BudgetNUSD), BudgetNUSD: u.BudgetNUSD,
		TotalSpentUSD: usd(u.TotalSpentNUSD), TotalSpentNUSD: u.TotalSpentNUSD,
		HeldUSD: usd(u.HeldNUSD), HeldNUSD: u.HeldNUSD,
		AvailableUSD: usdPtr(available), AvailableNUSD: available,
		SpentUSD: usd(u.SpentNUSD), SpentNUSD: u.SpentNUSD,
		Requests: u.Requests, Groups: make([]usageGroup, len(u.Groups)),
	}
	for i, g := range u.Groups {
		out.Groups[i] = usageGroup{
			Alias: g.Alias, Provider: g.Provider, UpstreamModel: g.UpstreamModel, Requests: g.Requests,
			PromptTokens: g.PromptTokens, CompletionTokens: g.CompletionTokens,
			SpentUSD: usd(g.SpentNUSD), SpentNUSD: g.SpentNUSD,
		}
	}
	writeJSON(w, http.StatusOK, out)
}

type ledgerResponse struct {
	Data       []ledger.Entry `json:"data"`
	NextBefore *int64         `json:"next_before"` // pass as ?before= for the next page; null on the last
}

func (s *Server) handleKeyLedger(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		badRequest("invalid_id", "Invalid key id.").write(w, r)
		return
	}
	q := r.URL.Query()
	limit := defaultLedgerPage
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLedgerPage {
			badRequest("invalid_limit", "limit must be between 1 and %d.", maxLedgerPage).write(w, r)
			return
		}
	}
	var before int64
	if v := q.Get("before"); v != "" {
		if before, err = strconv.ParseInt(v, 10, 64); err != nil || before < 1 {
			badRequest("invalid_before", "before must be an entry id.").write(w, r)
			return
		}
	}

	entries, err := s.opts.Reports.Entries(r.Context(), id, limit, before)
	if err != nil {
		reportFailed(w, r, err)
		return
	}
	out := ledgerResponse{Data: entries}
	if len(entries) == limit {
		out.NextBefore = &entries[len(entries)-1].ID
	}
	writeJSON(w, http.StatusOK, out)
}
