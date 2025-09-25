package apple

// Product represents a product configuration for browser monitoring
type Product struct {
	Code    string // Product code (e.g., "MG034CH/A")
	Name    string // Product name (e.g., "iPhone 17 Pro Max 256GB")
	Storage string // Storage capacity (e.g., "256GB")
	Color   string // Product color (e.g., "沙色钛金属")
	Model   string // Model identifier (e.g., "iPhone17,2")
}

// Store represents a store configuration for browser monitoring
type Store struct {
	Code    string // Store code (e.g., "R639")
	Name    string // Store name (e.g., "Apple 珠江新城")
	City    string // City name (e.g., "广州")
	Address string // Store address
}