package main

import "fmt"

func main() {
	arr := []int{0, 1, 3, 4}
	fmt.Println(get(2, arr))
	fmt.Println(get(-1, arr))
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
