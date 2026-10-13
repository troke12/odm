package engine

// NewPlan splits [0,total) into at most conns chunks. total<=0 (unknown size)
// yields a single open-ended chunk that is filled sequentially.
func NewPlan(total int64, conns int) []*Chunk {
	if conns < 1 {
		conns = 1
	}
	if total <= 0 {
		return []*Chunk{{ID: 0, Start: 0, End: -1}}
	}
	size := (total + int64(conns) - 1) / int64(conns)
	if size < MinChunkSize {
		size = MinChunkSize
	}
	var chunks []*Chunk
	for start, id := int64(0), 0; start < total; id++ {
		end := start + size - 1
		if end >= total {
			end = total - 1
		}
		chunks = append(chunks, &Chunk{ID: id, Start: start, End: end})
		start = end + 1
	}
	// Fold a too-small tail into the previous chunk instead of scheduling a
	// tiny request, matching the minimum-chunk behaviour.
	if n := len(chunks); n > 1 && chunks[n-1].Size() < MinChunkSize {
		chunks[n-2].End = chunks[n-1].End
		chunks = chunks[:n-1]
	}
	return chunks
}

// Split carves the tail half off one incomplete chunk and returns it as a
// new chunk for a freed connection. The chunk that will take the longest to
// finish is chosen when two paces are known and the slower one will take at
// least twice as long. Otherwise the largest remainder is chosen.
// It reports false when no chunk is big enough to split safely.
func Split(chunks []*Chunk, minChunk int64) (*Chunk, bool) {
	var largest, slow *Chunk
	var bestRemain int64
	var slowETA, fastETA float64
	paced := 0
	maxID := -1
	for _, c := range chunks {
		if c.ID > maxID {
			maxID = c.ID
		}
		if c.Complete() {
			continue
		}
		r := c.Remaining()
		if r < 2*minChunk {
			continue
		}
		if r > bestRemain {
			bestRemain, largest = r, c
		}
		if c.pace <= 0 {
			continue
		}
		eta := float64(r) / float64(c.pace)
		paced++
		if slow == nil || eta > slowETA {
			slow, slowETA = c, eta
		}
		if paced == 1 || eta < fastETA {
			fastETA = eta
		}
	}
	victim := largest
	if paced >= 2 && slow != nil && fastETA > 0 && slowETA >= 2*fastETA {
		victim = slow
	}
	if victim == nil {
		return nil, false
	}
	mid := victim.Next() + victim.Remaining()/2
	if mid <= victim.Start || mid > victim.End {
		return nil, false
	}
	nc := &Chunk{ID: maxID + 1, Start: mid, End: victim.End}
	victim.End = mid - 1
	return nc, true
}
