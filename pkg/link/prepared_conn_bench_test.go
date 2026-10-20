package link

import (
	"fmt"
	"math/rand"
	"testing"
)

// Benchmarking Tests for prepared statement deallocation
const MAX_STATEMENTS = 100

func BenchmarkMaxDeallocationOld(b *testing.B) {
	connPreparedStmts := make(map[string]ConnPreparedStmt)

	// Create 100,000 statements with random names
	const numStmts = 100000
	stmts := make([]string, numStmts)
	for i := 0; i < numStmts; i++ {
		stmts[i] = fmt.Sprintf("stmt_%d_%d", i, rand.Int63())
	}

	b.ReportAllocs()
	b.ResetTimer()

	numToStartDeallocating := 200
	for n := 0; n < b.N; n++ {
		// Add all statements to map
		for i, stmt := range stmts {
			connPreparedStmts[stmt] = ConnPreparedStmt{}

			// Every 10 stmts, choose a random stmt to promote (increment usage count)
			if (i+1)%10 == 0 && i > 0 {
				randomIdx := rand.Intn(i + 1)
				randomStmt := stmts[randomIdx]
				if metaData, exists := connPreparedStmts[randomStmt]; exists {
					metaData.UsageCount++
					connPreparedStmts[randomStmt] = metaData
				}
			}

			// Every 200 stmts, peek (shouldDeallocateOld) and evict 100(delete)
			if (i+1)%numToStartDeallocating == 0 {
				stmtsToDeallocateCount := numToStartDeallocating - MAX_STATEMENTS
				for _, stmtName := range StmtsToDeallocate(connPreparedStmts, stmtsToDeallocateCount, 0) {
					delete(connPreparedStmts, stmtName)
				}
			}
		}
	}
}

func BenchmarkAddOld(b *testing.B) {
	connPreparedStmts := make(map[string]ConnPreparedStmt)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		connPreparedStmts[fmt.Sprintf("stmt_%d_%d", n, rand.Int63())] = ConnPreparedStmt{}
	}
}

func BenchmarkPromoteOld(b *testing.B) {
	connPreparedStmts := make(map[string]ConnPreparedStmt)
	key := "12345"
	connPreparedStmts[key] = ConnPreparedStmt{}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if metaData, exists := connPreparedStmts[key]; exists {
			metaData.UsageCount++
			connPreparedStmts[key] = metaData
		}
	}
}

func BenchmarkPeekEvictOld(b *testing.B) {
	const numStmts = 100000
	connPreparedStmts := make(map[string]ConnPreparedStmt)
	for i := 0; i < numStmts; i++ {
		newKey := fmt.Sprintf("stmt_%d_%d", i, rand.Int63())
		connPreparedStmts[newKey] = ConnPreparedStmt{}
	}

	b.ReportAllocs()
	b.ResetTimer()
	stmtsToDeallocateCount := numStmts - MAX_STATEMENTS
	for n := 0; n < b.N; n++ {
		for _, stmtName := range StmtsToDeallocate(connPreparedStmts, stmtsToDeallocateCount, 0) {
			delete(connPreparedStmts, stmtName)
		}
	}
}
