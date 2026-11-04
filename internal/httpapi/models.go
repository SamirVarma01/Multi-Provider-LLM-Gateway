package httpapi

import (
	"net/http"
)

type modelList struct {
	Object string       `json:"object"` // "list"
	Data   []modelEntry `json:"data"`
}

type modelEntry struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"` // "model"
	Created int64        `json:"created"`
	OwnedBy string       `json:"owned_by"`
	Pricing modelPricing `json:"pricing"`
}

// modelPricing is the highest price across an alias's candidates: an upper
// bound on what a request can cost per token.
type modelPricing struct {
	InputPerMTokUSD  float64 `json:"input_per_mtok_usd"`
	OutputPerMTokUSD float64 `json:"output_per_mtok_usd"`
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	out := modelList{Object: "list", Data: []modelEntry{}}
	for _, m := range s.opts.Router.Models() {
		var in, outp int64
		for _, c := range m.Candidates {
			in = max(in, c.Target.Price.InputMicroPerM)
			outp = max(outp, c.Target.Price.OutputMicroPerM)
		}
		out.Data = append(out.Data, modelEntry{
			ID:      m.Alias,
			Object:  "model",
			OwnedBy: "llm-gateway",
			Pricing: modelPricing{InputPerMTokUSD: float64(in) / 1e6, OutputPerMTokUSD: float64(outp) / 1e6},
		})
	}
	writeJSON(w, http.StatusOK, out)
}
