package main

import "sync"

func main() {
	mu := sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 100; i++ {
		mu.Unlock()
		mu.Lock()
	}

}
func get(k int, arr []int) (int, bool) {
	for i, j := 0, len(arr)-1; i < j; {
		mid := i + j>>1
		if arr[mid] >= k {
			j = mid
		} else {
			i = mid + 1
		}
		return i, i >= 0 && i < len(arr) && arr[i] == k
	}
	return 0, false
}
