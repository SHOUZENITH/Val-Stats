package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

// --- 1. Struct Definitions ---
type DraftAction struct {
	Team   string `json:"team"`
	Action string `json:"action"`
	Map    string `json:"map"`
}

type PlayerStat struct {
	PlayerID    string `json:"player_id"`
	PlayerName  string `json:"player_name"`
	TeamID      string `json:"team_id"`
	Agent       string `json:"agent"`
	ACS         string `json:"acs"`
	Kills       string `json:"kills"`
	Deaths      string `json:"deaths"`
	Assists     string `json:"assists"`
	FirstKills  string `json:"first_kills"`
	FirstDeaths string `json:"first_deaths"`
}

type MapResult struct {
	MapName     string       `json:"map_name"`
	TeamAScore  string       `json:"team_a_score"`
	TeamBScore  string       `json:"team_b_score"`
	PlayerStats []PlayerStat `json:"player_stats"`
}

type ScrapedMatch struct {
	MatchID    string        `json:"match_id"`
	Tournament string        `json:"tournament"`
	Date       string        `json:"date"`
	TeamAID    string        `json:"team_a_id"`
	TeamA      string        `json:"team_a"`
	TeamBID    string        `json:"team_b_id"`
	TeamB      string        `json:"team_b"`
	DraftPhase []DraftAction `json:"draft_phase"`
	MapResults []MapResult   `json:"map_results"`
}

// Helper function to safely convert strings like "+13" or "20%" to integers
func cleanInt(s string) int {
	s = strings.ReplaceAll(s, "+", "")
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "%", "")
	s = strings.TrimSpace(s)
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return val
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL environment variable is not set!")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("Failed to open DB:", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Failed to ping Supabase:", err)
	}
	fmt.Println("Connected to Supabase!")

	// --- 3. API Router Setup ---
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.POST("/api/matches/ingest", func(c *gin.Context) {
		var matches []ScrapedMatch
		if err := c.ShouldBindJSON(&matches); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		for _, match := range matches {
			fmt.Printf("Inserting Match ID: %s...\n", match.MatchID)

			// Start a Database Transaction
			tx, err := db.Begin()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start transaction"})
				return
			}

			// 1. Upsert Teams
			_, err = tx.Exec(`INSERT INTO teams (id, name) VALUES ($1, $2), ($3, $4) 
				ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
				match.TeamAID, match.TeamA, match.TeamBID, match.TeamB)
			if err != nil {
				tx.Rollback()
				continue
			}

			// 2. Insert Match
			_, err = tx.Exec(`INSERT INTO matches (id, tournament, match_date, team_a_id, team_b_id) 
				VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
				match.MatchID, match.Tournament, match.Date, match.TeamAID, match.TeamBID)
			if err != nil {
				tx.Rollback()
				continue
			}

			// 3. Insert Draft Phase
			for stepIndex, draft := range match.DraftPhase {
				var mapID int
				err = tx.QueryRow(`INSERT INTO maps (name) VALUES ($1) 
					ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`, draft.Map).Scan(&mapID)

				var draftTeamID *string
				if strings.Contains(strings.ToLower(match.TeamA), strings.ToLower(draft.Team)) {
					draftTeamID = &match.TeamAID
				} else if strings.Contains(strings.ToLower(match.TeamB), strings.ToLower(draft.Team)) {
					draftTeamID = &match.TeamBID
				}

				tx.Exec(`INSERT INTO match_drafts (match_id, step_number, team_id, action, map_id) 
					VALUES ($1, $2, $3, $4, $5)`, match.MatchID, stepIndex+1, draftTeamID, draft.Action, mapID)
			}

			// 4. Insert Maps & Player Stats
			for _, result := range match.MapResults {
				var mapID int
				err = tx.QueryRow(`INSERT INTO maps (name) VALUES ($1) 
					ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`, result.MapName).Scan(&mapID)

				var mapResultID int
				err = tx.QueryRow(`INSERT INTO map_results (match_id, map_id, team_a_score, team_b_score) 
					VALUES ($1, $2, $3, $4) RETURNING id`,
					match.MatchID, mapID, cleanInt(result.TeamAScore), cleanInt(result.TeamBScore)).Scan(&mapResultID)
				
				if err != nil {
					tx.Rollback()
					break
				}

				for _, stat := range result.PlayerStats {
					// Upsert Player
					tx.Exec(`INSERT INTO players (id, name, team_id) VALUES ($1, $2, $3) 
						ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`,
						stat.PlayerID, stat.PlayerName, stat.TeamID)

					// Upsert Agent
					var agentID int
					err = tx.QueryRow(`INSERT INTO agents (name, role) VALUES ($1, 'Unknown') 
						ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id`, stat.Agent).Scan(&agentID)

					// Insert Final Player Stats
					tx.Exec(`INSERT INTO player_stats 
						(map_result_id, player_id, team_id, agent_id, acs, kills, deaths, assists, first_kills, first_deaths) 
						VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
						mapResultID, stat.PlayerID, stat.TeamID, agentID, 
						cleanInt(stat.ACS), cleanInt(stat.Kills), cleanInt(stat.Deaths), cleanInt(stat.Assists), 
						cleanInt(stat.FirstKills), cleanInt(stat.FirstDeaths))
				}
			}
			
			// Commit the transaction
			if err := tx.Commit(); err != nil {
				log.Println("Failed to commit transaction:", err)
			} else {
				fmt.Println("Successfully saved Match ID:", match.MatchID)
			}
		}

		c.JSON(http.StatusOK, gin.H{"status": "success"})
	})

	// --- 4. Render Port Configuration ---
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Fallback for local testing
	}

	fmt.Printf("Go Backend listening on port :%s\n", port)
	router.Run(":" + port)
}