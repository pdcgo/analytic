package analytic_bigquery

func ParseArgs[T any](keys []string, args []T, defValue T) map[string]T {
	data := map[string]T{}

	for index, k := range keys {
		data[k] = defValue

		if len(args) > index {
			data[k] = args[index]
		}
	}

	return data
}
