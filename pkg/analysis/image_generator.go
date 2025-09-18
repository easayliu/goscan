package analysis

import (
	"bytes"
	"context"
	"fmt"
	"goscan/pkg/logger"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"

	"github.com/fogleman/gg"
	"go.uber.org/zap"
	"golang.org/x/image/font"
	"golang.org/x/image/math/fixed"
)

// ImageConfig defines image generation configuration
type ImageConfig struct {
	Width       int
	Height      int
	Padding     int
	CardGap     int
	CardPadding int
	MaxSizeMB   float64
}

// DefaultImageConfig returns optimized image configuration for better readability
func DefaultImageConfig() *ImageConfig {
	return &ImageConfig{
		Width:       1400,  // increased width for better layout
		Height:      0,     // dynamic
		Padding:     50,    // increased padding for better spacing
		CardGap:     30,    // increased gap between cards
		CardPadding: 25,    // increased internal card padding
		MaxSizeMB:   8.0,   // increased size limit for higher quality
	}
}

// ImageGenerator generates cost report images
type ImageGenerator struct {
	config      *ImageConfig
	colors      ColorScheme
	fontManager *FontManager
}

// ColorScheme defines color scheme for the image
type ColorScheme struct {
	Background  color.Color
	CardBG      color.Color
	Primary     color.Color
	Secondary   color.Color
	Success     color.Color
	Warning     color.Color
	Danger      color.Color
	Border      color.Color
}

// DefaultColorScheme returns optimized color scheme for better contrast and readability
func DefaultColorScheme() ColorScheme {
	return ColorScheme{
		Background: color.RGBA{245, 247, 250, 255}, // Softer light gray background
		CardBG:     color.RGBA{255, 255, 255, 255}, // Pure white cards
		Primary:    color.RGBA{17, 24, 39, 255},    // Darker text for better contrast
		Secondary:  color.RGBA{75, 85, 99, 255},    // Improved gray text contrast
		Success:    color.RGBA{16, 185, 129, 255},  // Modern green with better visibility
		Warning:    color.RGBA{245, 158, 11, 255},  // Enhanced orange-yellow for warnings
		Danger:     color.RGBA{239, 68, 68, 255},   // Modern red with better contrast
		Border:     color.RGBA{209, 213, 219, 255}, // Subtle border color
	}
}

// NewImageGenerator creates a new image generator
func NewImageGenerator(config *ImageConfig) *ImageGenerator {
	if config == nil {
		config = DefaultImageConfig()
	}
	
	return &ImageGenerator{
		config:      config,
		colors:      DefaultColorScheme(),
		fontManager: GetFontManager(),
	}
}

// NewModernAppleReportGenerator creates a modern Apple-style report generator for compatibility
func NewModernAppleReportGenerator() *ImageGenerator {
	return NewImageGenerator(DefaultImageConfig())
}

// GenerateReport generates a cost report image using gg library
func (g *ImageGenerator) GenerateReport(result *CostAnalysisResult) ([]byte, error) {
	if result == nil {
		return nil, fmt.Errorf("analysis result cannot be nil")
	}

	// Calculate dynamic height
	height := g.calculateHeight(result)
	g.config.Height = height

	// Create gg context
	dc := gg.NewContext(g.config.Width, g.config.Height)
	
	// Set background
	dc.SetColor(g.colors.Background)
	dc.Clear()
	
	// Load and set font
	if err := g.loadFont(dc); err != nil {
		logger.Warn("failed to load font, using default", zap.Error(err))
	}

	y := float64(g.config.Padding)

	// Draw sections
	y = g.drawHeaderGG(dc, result, y)
	y = g.drawSummaryCardGG(dc, result.TotalCost, y)
	
	for _, provider := range result.Providers {
		y = g.drawProviderCardGG(dc, provider, y)
	}

	if len(result.Alerts) > 0 {
		y = g.drawAlertsCardGG(dc, result.Alerts, y)
	}

	// Encode to PNG
	return g.encodePNGGG(dc)
}

// calculateHeight calculates required image height with improved spacing
func (g *ImageGenerator) calculateHeight(result *CostAnalysisResult) int {
	height := g.config.Padding * 2

	// Header with more breathing room
	height += 120 // Increased for additional date comparison line

	// Summary card with enhanced height
	height += 170 // Increased for showing both yesterday and day before costs

	// Provider cards with better spacing
	for _, provider := range result.Providers {
		height += 80 // Base card height for single line header + separator
		if len(provider.Products) > 0 {
			// Match the actual product list layout: 30 for header + 40 per product
			height += 30 + len(provider.Products)*40
		}
		height += g.config.CardGap
	}

	// Alerts card with better spacing
	if len(result.Alerts) > 0 {
		height += 80 + len(result.Alerts)*35 // Increased alert spacing
		height += g.config.CardGap
	}

	return height
}

// drawHeader draws the header section with improved typography
func (g *ImageGenerator) drawHeader(img draw.Image, result *CostAnalysisResult, y int) int {
	// Title with enhanced spacing
	title := "云服务费用日报"
	g.drawTextWithFont(img, title, g.config.Padding, y+5, g.colors.Primary, FontSizeXLarge)
	
	// Date with better spacing and improved formatting
	y += 45
	date := fmt.Sprintf("报告日期: %s", result.Date.Format("2006年01月02日"))
	g.drawTextWithFont(img, date, g.config.Padding, y, g.colors.Secondary, FontSizeLarge)
	
	// Add generation timestamp for transparency
	y += 25
	generatedAt := fmt.Sprintf("生成时间: %s", result.GeneratedAt.Format("15:04:05"))
	g.drawTextWithFont(img, generatedAt, g.config.Padding, y, g.colors.Secondary, FontSizeMedium)
	
	return y + 30
}

// loadFont loads Chinese font for gg context
func (g *ImageGenerator) loadFont(dc *gg.Context) error {
	// Try to load font from multiple sources
	fontPaths := []string{
		"assets/fonts/SourceHanSansSC-Regular.otf",
		"./assets/fonts/SourceHanSansSC-Regular.otf",
		"/System/Library/Fonts/PingFang.ttc",
		"/System/Library/Fonts/STHeiti Light.ttc", 
		"/System/Library/Fonts/Helvetica.ttc",
		"/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
		"/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
		"C:\\Windows\\Fonts\\simsun.ttc",
		"C:\\Windows\\Fonts\\msyh.ttc",
	}
	
	for _, fontPath := range fontPaths {
		if err := dc.LoadFontFace(fontPath, 14); err == nil {
			logger.Info("successfully loaded font", zap.String("path", fontPath))
			return nil
		}
	}
	
	return fmt.Errorf("no suitable font found")
}

// drawSummaryCard draws the summary card with enhanced visual hierarchy
func (g *ImageGenerator) drawSummaryCard(img draw.Image, total *CostMetric, y int) int {
	if total == nil {
		return y
	}

	cardHeight := 120
	g.drawCardWithShadow(img, g.config.Padding, y, g.config.Width-g.config.Padding*2, cardHeight)

	contentX := g.config.Padding + g.config.CardPadding
	contentY := y + g.config.CardPadding + 5

	// Card title
	g.drawTextWithFont(img, "费用概览", contentX, contentY, g.colors.Secondary, FontSizeMedium)
	contentY += 30

	// Total cost with enhanced formatting
	costStr := fmt.Sprintf("总费用: ¥%.2f", total.TodayCost)
	g.drawTextWithFont(img, costStr, contentX, contentY, g.colors.Primary, FontSizeXLarge)

	// Change indicator with better spacing
	contentY += 35
	if total.ChangePercent > 0 {
		changeStr := fmt.Sprintf("较昨日上升 %.1f%%", total.ChangePercent)
		g.drawTextWithFont(img, changeStr, contentX, contentY, g.colors.Danger, FontSizeLarge)
	} else if total.ChangePercent < 0 {
		changeStr := fmt.Sprintf("较昨日下降 %.1f%%", math.Abs(total.ChangePercent))
		g.drawTextWithFont(img, changeStr, contentX, contentY, g.colors.Success, FontSizeLarge)
	} else {
		g.drawTextWithFont(img, "与昨日持平", contentX, contentY, g.colors.Secondary, FontSizeLarge)
	}

	return y + cardHeight + g.config.CardGap
}

// drawProviderCard draws a provider card with enhanced visual design
func (g *ImageGenerator) drawProviderCard(img draw.Image, provider *ProviderCostMetric, y int) int {
	cardHeight := 80
	if len(provider.Products) > 0 {
		cardHeight += len(provider.Products)*30 + 45 // Increased spacing for product list
	}

	g.drawCard(img, g.config.Padding, y, g.config.Width-g.config.Padding*2, cardHeight)

	contentX := g.config.Padding + g.config.CardPadding
	contentY := y + g.config.CardPadding + 5

	// Provider header with visual separator
	providerText := provider.DisplayName
	g.drawTextWithFont(img, providerText, contentX, contentY, g.colors.Primary, FontSizeXLarge)
	
	// Cost amount with better positioning
	costText := fmt.Sprintf("¥%.2f", provider.TotalCost.TodayCost)
	midX := g.config.Width/2 + 100
	g.drawTextWithFont(img, costText, midX, contentY, g.colors.Primary, FontSizeXLarge)

	// Change indicator with enhanced styling
	if provider.TotalCost.ChangePercent != 0 {
		rightX := g.config.Width - g.config.Padding - g.config.CardPadding - 20
		changeColor := g.colors.Success
		changePrefix := "-"
		if provider.TotalCost.ChangePercent > 0 {
			changeColor = g.colors.Danger
			changePrefix = "+"
		}
		changeStr := fmt.Sprintf("%s%.1f%%", changePrefix, math.Abs(provider.TotalCost.ChangePercent))
		g.drawTextRightWithFont(img, changeStr, rightX, contentY, changeColor, FontSizeLarge)
	}

	// Horizontal separator line
	contentY += 35
	g.drawHorizontalLine(img, contentX, contentY, g.config.Width-g.config.Padding*2-g.config.CardPadding*2, g.colors.Border)
	contentY += 10

	// Product details with improved spacing
	if len(provider.Products) > 0 {
		g.drawProductList(img, provider.Products, contentX, contentY)
	}

	return y + cardHeight + g.config.CardGap
}

// drawProductList draws product details with improved readability
func (g *ImageGenerator) drawProductList(img draw.Image, products []*CostMetric, x, y int) {
	// Add section header
	g.drawTextWithFont(img, "产品明细:", x, y, g.colors.Secondary, FontSizeMedium)
	y += 25
	
	for i, product := range products {
		currentY := y + i*30
		
		// Product name and cost with better formatting
		productText := fmt.Sprintf("• %s", product.Name)
		g.drawTextWithFont(img, productText, x+10, currentY, g.colors.Primary, FontSizeMedium)
		
		// Cost amount
		costText := fmt.Sprintf("¥%.2f", product.TodayCost)
		midX := g.config.Width/2 + 50
		g.drawTextWithFont(img, costText, midX, currentY, g.colors.Primary, FontSizeMedium)
		
		// Change indicator with enhanced visibility
		if math.Abs(product.ChangePercent) > 0.1 {
			rightX := g.config.Width - g.config.Padding - g.config.CardPadding - 20
			changeColor := g.colors.Success
			changePrefix := "-"
			if product.ChangePercent > 0 {
				changeColor = g.colors.Warning
				changePrefix = "+"
			}
			changeStr := fmt.Sprintf("%s%.1f%%", changePrefix, math.Abs(product.ChangePercent))
			g.drawTextRightWithFont(img, changeStr, rightX, currentY, changeColor, FontSizeMedium)
		}
	}
}

// drawAlertsCard draws alerts card with enhanced warning design
func (g *ImageGenerator) drawAlertsCard(img draw.Image, alerts []string, y int) int {
	cardHeight := 60 + len(alerts)*30
	g.drawWarningCard(img, g.config.Padding, y, g.config.Width-g.config.Padding*2, cardHeight)

	contentX := g.config.Padding + g.config.CardPadding
	contentY := y + g.config.CardPadding + 5

	// Warning icon and title
	warningHeader := "费用告警"
	g.drawTextWithFont(img, warningHeader, contentX, contentY, g.colors.Warning, FontSizeXLarge)

	// Separator line
	contentY += 30
	g.drawHorizontalLine(img, contentX, contentY, g.config.Width-g.config.Padding*2-g.config.CardPadding*2, g.colors.Warning)
	contentY += 15

	// Alert items with enhanced formatting
	for i, alert := range alerts {
		alertText := fmt.Sprintf("• %s", alert)
		g.drawTextWithFont(img, alertText, contentX+10, contentY, g.colors.Danger, FontSizeLarge)
		contentY += 30
		
		// Add spacing between multiple alerts
		if i < len(alerts)-1 {
			contentY += 5
		}
	}

	return y + cardHeight + g.config.CardGap
}

// Helper drawing functions

func (g *ImageGenerator) drawCard(img draw.Image, x, y, width, height int) {
	// Card background
	cardRect := image.Rect(x, y, x+width, y+height)
	draw.Draw(img, cardRect, &image.Uniform{g.colors.CardBG}, image.Point{}, draw.Src)
	
	// Enhanced border with rounded corners effect
	g.drawEnhancedBorder(img, x, y, width, height, g.colors.Border)
}

// drawCardWithShadow draws a card with subtle shadow effect for better visual hierarchy
func (g *ImageGenerator) drawCardWithShadow(img draw.Image, x, y, width, height int) {
	// Subtle shadow effect (offset by 2 pixels)
	shadowColor := color.RGBA{0, 0, 0, 30} // Very light shadow
	shadowRect := image.Rect(x+2, y+2, x+width+2, y+height+2)
	draw.Draw(img, shadowRect, &image.Uniform{shadowColor}, image.Point{}, draw.Src)
	
	// Main card
	cardRect := image.Rect(x, y, x+width, y+height)
	draw.Draw(img, cardRect, &image.Uniform{g.colors.CardBG}, image.Point{}, draw.Src)
	
	// Enhanced border
	g.drawEnhancedBorder(img, x, y, width, height, g.colors.Border)
}

func (g *ImageGenerator) drawBorder(img draw.Image, x, y, width, height int, c color.Color) {
	// Top and bottom
	for i := 0; i < width; i++ {
		img.Set(x+i, y, c)
		img.Set(x+i, y+height-1, c)
	}
	// Left and right
	for i := 0; i < height; i++ {
		img.Set(x, y+i, c)
		img.Set(x+width-1, y+i, c)
	}
}

// drawEnhancedBorder draws a more subtle border with corner rounding effect
func (g *ImageGenerator) drawEnhancedBorder(img draw.Image, x, y, width, height int, c color.Color) {
	// Draw main border lines
	g.drawBorder(img, x, y, width, height, c)
	
	// Add subtle corner rounding by removing corner pixels
	backgroundColor := g.colors.Background
	
	// Top-left corner
	img.Set(x, y, backgroundColor)
	img.Set(x+1, y, c)
	img.Set(x, y+1, c)
	
	// Top-right corner  
	img.Set(x+width-1, y, backgroundColor)
	img.Set(x+width-2, y, c)
	img.Set(x+width-1, y+1, c)
	
	// Bottom-left corner
	img.Set(x, y+height-1, backgroundColor)
	img.Set(x+1, y+height-1, c)
	img.Set(x, y+height-2, c)
	
	// Bottom-right corner
	img.Set(x+width-1, y+height-1, backgroundColor)
	img.Set(x+width-2, y+height-1, c)
	img.Set(x+width-1, y+height-2, c)
}

// drawWarningCard draws a card with warning styling
func (g *ImageGenerator) drawWarningCard(img draw.Image, x, y, width, height int) {
	// Warning card background with slight tint
	warningBG := color.RGBA{255, 252, 240, 255} // Very light yellow tint
	cardRect := image.Rect(x, y, x+width, y+height)
	draw.Draw(img, cardRect, &image.Uniform{warningBG}, image.Point{}, draw.Src)
	
	// Warning border with orange color
	warningBorder := color.RGBA{245, 158, 11, 180} // Semi-transparent warning color
	g.drawEnhancedBorder(img, x, y, width, height, warningBorder)
	
	// Add left accent bar for visual emphasis
	for i := 0; i < height; i++ {
		img.Set(x+1, y+i, g.colors.Warning)
		img.Set(x+2, y+i, g.colors.Warning)
		img.Set(x+3, y+i, g.colors.Warning)
	}
}

// drawHorizontalLine draws a horizontal separator line
func (g *ImageGenerator) drawHorizontalLine(img draw.Image, x, y, width int, c color.Color) {
	for i := 0; i < width; i++ {
		img.Set(x+i, y, c)
	}
}

// drawTextWithFont draws text using the font manager
func (g *ImageGenerator) drawTextWithFont(img draw.Image, text string, x, y int, c color.Color, fontSize FontSize) {
	face := g.fontManager.GetFont(fontSize)
	
	// Try to use FreetypeFace if available
	if freetypeFace, ok := face.(*FreetypeFace); ok {
		// Use freetype's direct text drawing
		err := freetypeFace.DrawText(img, text, x, y, c)
		if err == nil {
			return // Successfully drawn with freetype
		}
	}
	
	// Fallback to standard font.Drawer
	point := fixed.Point26_6{
		X: fixed.I(x),
		Y: fixed.I(y + int(fontSize)), // Baseline adjustment based on font size
	}

	d := &font.Drawer{
		Dst:  img,
		Src:  &image.Uniform{c},
		Face: face,
		Dot:  point,
	}

	d.DrawString(text)
}

// drawTextRightWithFont draws right-aligned text using the font manager
func (g *ImageGenerator) drawTextRightWithFont(img draw.Image, text string, x, y int, c color.Color, fontSize FontSize) {
	face := g.fontManager.GetFont(fontSize)
	
	// Try to measure text width accurately
	var textWidth int
	if _, ok := face.(*FreetypeFace); ok {
		// For freetype, use approximate width based on character count and font size
		textWidth = len([]rune(text)) * int(float64(fontSize) * 0.6) // Approximate character width
	} else {
		// Use standard measurement
		measured := font.MeasureString(face, text)
		textWidth = int(measured.Round())
	}
	
	adjustedX := x - textWidth
	g.drawTextWithFont(img, text, adjustedX, y, c, fontSize)
}

// Legacy methods for compatibility
func (g *ImageGenerator) drawText(img draw.Image, text string, x, y int, c color.Color, size int) {
	g.drawTextWithFont(img, text, x, y, c, FontSizeMedium)
}

func (g *ImageGenerator) drawTextRight(img draw.Image, text string, x, y int, c color.Color, size int) {
	g.drawTextRightWithFont(img, text, x, y, c, FontSizeMedium)
}

// encodePNG encodes image to PNG with optimal settings for readability
func (g *ImageGenerator) encodePNG(img image.Image) ([]byte, error) {
	var buf bytes.Buffer
	encoder := png.Encoder{
		CompressionLevel: png.BestCompression, // Keep best compression for file size
	}

	if err := encoder.Encode(&buf, img); err != nil {
		logger.Error("failed to encode PNG image", zap.Error(err))
		return nil, fmt.Errorf("failed to encode PNG image: %w", err)
	}

	data := buf.Bytes()
	
	// Enhanced size validation with detailed logging
	sizeMB := float64(len(data)) / 1024 / 1024
	logger.Info("generated PNG image", 
		zap.Float64("size_mb", sizeMB),
		zap.Int("width", g.config.Width),
		zap.Int("height", g.config.Height),
		zap.String("format", "PNG"))
		
	if sizeMB > g.config.MaxSizeMB {
		logger.Warn("PNG image exceeds size limit", 
			zap.Float64("actual_size_mb", sizeMB), 
			zap.Float64("limit_mb", g.config.MaxSizeMB),
			zap.String("recommendation", "consider reducing image dimensions or content"))
	}

	return data, nil
}

// GenerateImageAsync generates image asynchronously
func (g *ImageGenerator) GenerateImageAsync(ctx context.Context, result *CostAnalysisResult) (<-chan []byte, <-chan error) {
	imageChan := make(chan []byte, 1)
	errChan := make(chan error, 1)
	
	go func() {
		defer close(imageChan)
		defer close(errChan)
		
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
		}
		
		imageData, err := g.GenerateReport(result)
		if err != nil {
			errChan <- err
			return
		}
		
		imageChan <- imageData
	}()
	
	return imageChan, errChan
}

// New gg-based drawing functions

// drawHeaderGG draws the header section using gg
func (g *ImageGenerator) drawHeaderGG(dc *gg.Context, result *CostAnalysisResult, y float64) float64 {
	// Title
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 24)
	dc.SetColor(g.colors.Primary)
	title := "云服务费用日报 - 昨日报告"
	dc.DrawString(title, float64(g.config.Padding), y+25)
	
	// Date range - showing yesterday vs day before
	y += 50
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 18)
	dc.SetColor(g.colors.Primary)
	dateRange := fmt.Sprintf("统计日期: %s (对比 %s)", 
		result.Date.Format("2006年01月02日"),
		result.Date.AddDate(0, 0, -1).Format("01月02日"))
	dc.DrawString(dateRange, float64(g.config.Padding), y+20)
	
	// Generation timestamp with full date and time
	y += 30
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
	dc.SetColor(g.colors.Secondary)
	generatedAt := fmt.Sprintf("生成时间: %s", result.GeneratedAt.Format("2006-01-02 15:04:05"))
	dc.DrawString(generatedAt, float64(g.config.Padding), y+20)
	
	return y + 35
}

// drawSummaryCardGG draws the summary card using gg
func (g *ImageGenerator) drawSummaryCardGG(dc *gg.Context, total *CostMetric, y float64) float64 {
	if total == nil {
		return y
	}

	cardHeight := 150.0  // Height for all content
	g.drawCardGG(dc, float64(g.config.Padding), y, float64(g.config.Width-g.config.Padding*2), cardHeight)

	contentX := float64(g.config.Padding + g.config.CardPadding)
	contentY := y + float64(g.config.CardPadding)

	// Card title
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
	dc.SetColor(g.colors.Secondary)
	dc.DrawString("费用概览", contentX, contentY+16)
	contentY += 35

	// Yesterday's cost (main figure)
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 24)
	dc.SetColor(g.colors.Primary)
	yesterdayStr := fmt.Sprintf("昨日费用: ¥%.2f", total.TodayCost)
	dc.DrawString(yesterdayStr, contentX, contentY+24)
	
	// Day before yesterday's cost for comparison
	contentY += 35
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 18)
	dc.SetColor(g.colors.Secondary)
	dayBeforeStr := fmt.Sprintf("前日费用: ¥%.2f", total.YesterdayCost)
	dc.DrawString(dayBeforeStr, contentX, contentY+18)

	// Change indicator with clear comparison
	contentY += 30
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 17)
	if total.ChangePercent > 0 {
		changeStr := fmt.Sprintf("较前日上升 ¥%.2f (+%.1f%%)", 
			total.TodayCost - total.YesterdayCost, total.ChangePercent)
		dc.SetColor(g.colors.Danger)
		dc.DrawString(changeStr, contentX, contentY+17)
	} else if total.ChangePercent < 0 {
		changeStr := fmt.Sprintf("较前日下降 ¥%.2f (-%.1f%%)", 
			math.Abs(total.TodayCost - total.YesterdayCost), math.Abs(total.ChangePercent))
		dc.SetColor(g.colors.Success)
		dc.DrawString(changeStr, contentX, contentY+17)
	} else {
		dc.SetColor(g.colors.Secondary)
		dc.DrawString("与前日持平", contentX, contentY+17)
	}

	return y + cardHeight + float64(g.config.CardGap)
}

// drawProviderCardGG draws a provider card using gg
func (g *ImageGenerator) drawProviderCardGG(dc *gg.Context, provider *ProviderCostMetric, y float64) float64 {
	// Calculate card height more accurately
	cardHeight := 80.0 // Reduced base height for single line header
	if len(provider.Products) > 0 {
		// Add height for product section header and products
		cardHeight += float64(30 + len(provider.Products)*40) // 30 for header, 40 per product
	}

	g.drawCardGG(dc, float64(g.config.Padding), y, float64(g.config.Width-g.config.Padding*2), cardHeight)

	contentX := float64(g.config.Padding + g.config.CardPadding)
	contentY := y + float64(g.config.CardPadding)

	// Provider name (left aligned)
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 18)
	dc.SetColor(g.colors.Primary)
	dc.DrawString(provider.DisplayName, contentX, contentY+20)
	
	// Yesterday's cost (center aligned)
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
	dc.SetColor(g.colors.Primary)
	costText := fmt.Sprintf("昨日: ¥%.2f", provider.TotalCost.TodayCost)
	midX := float64(g.config.Width/2 - 80)
	dc.DrawString(costText, midX, contentY+20)

	// Day before yesterday's cost (smaller, on same line)
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 14)
	dc.SetColor(g.colors.Secondary)
	prevCostText := fmt.Sprintf("(前日: ¥%.2f)", provider.TotalCost.YesterdayCost)
	dc.DrawString(prevCostText, midX+130, contentY+20)

	// Change indicator (right aligned on same line)
	if provider.TotalCost.ChangePercent != 0 {
		rightX := float64(g.config.Width - g.config.Padding - g.config.CardPadding - 40)
		changeColor := g.colors.Success
		changePrefix := "-"
		if provider.TotalCost.ChangePercent > 0 {
			changeColor = g.colors.Danger
			changePrefix = "+"
		}
		dc.SetColor(changeColor)
		dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
		changeStr := fmt.Sprintf("%s%.1f%%", changePrefix, math.Abs(provider.TotalCost.ChangePercent))
		// Draw right-aligned text with safety margin
		textWidth, _ := dc.MeasureString(changeStr)
		dc.DrawString(changeStr, rightX-textWidth, contentY+20)
	}

	// Separator line
	contentY += 35
	g.drawHorizontalLineGG(dc, contentX, contentY, float64(g.config.Width-g.config.Padding*2-g.config.CardPadding*2))
	contentY += 10

	// Product details
	if len(provider.Products) > 0 {
		g.drawProductListGG(dc, provider.Products, contentX, contentY)
	}

	return y + cardHeight + float64(g.config.CardGap)
}

// drawProductListGG draws product details using gg
func (g *ImageGenerator) drawProductListGG(dc *gg.Context, products []*CostMetric, x, y float64) {
	// Section header
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
	dc.SetColor(g.colors.Secondary)
	dc.DrawString("产品明细:", x, y+16)
	y += 35
	
	for i, product := range products {
		currentY := y + float64(i*40) // Proper spacing between products
		
		// Product name (left aligned)
		dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 15)
		dc.SetColor(g.colors.Primary)
		productText := fmt.Sprintf("• %s", product.Name)
		dc.DrawString(productText, x+10, currentY+15)
		
		// Yesterday's cost (center aligned)
		costText := fmt.Sprintf("昨日: ¥%.2f", product.TodayCost)
		midX := float64(g.config.Width/2 - 80)
		dc.DrawString(costText, midX, currentY+15)
		
		// Day before's cost (smaller, below or beside)
		dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 13)
		dc.SetColor(g.colors.Secondary)
		prevCostText := fmt.Sprintf("(前日: ¥%.2f)", product.YesterdayCost)
		dc.DrawString(prevCostText, midX+130, currentY+15)
		
		// Change indicator (right aligned with safety margin)
		if math.Abs(product.ChangePercent) > 0.1 {
			rightX := float64(g.config.Width - g.config.Padding - g.config.CardPadding - 40)
			changeColor := g.colors.Success
			changePrefix := "-"
			if product.ChangePercent > 0 {
				changeColor = g.colors.Warning
				changePrefix = "+"
			}
			dc.SetColor(changeColor)
			dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 14)
			changeStr := fmt.Sprintf("%s%.1f%%", changePrefix, math.Abs(product.ChangePercent))
			textWidth, _ := dc.MeasureString(changeStr)
			dc.DrawString(changeStr, rightX-textWidth, currentY+15)
		}
	}
}

// drawAlertsCardGG draws alerts card using gg
func (g *ImageGenerator) drawAlertsCardGG(dc *gg.Context, alerts []string, y float64) float64 {
	cardHeight := float64(80 + len(alerts)*35) // Adjusted for proper spacing
	g.drawWarningCardGG(dc, float64(g.config.Padding), y, float64(g.config.Width-g.config.Padding*2), cardHeight)

	contentX := float64(g.config.Padding + g.config.CardPadding)
	contentY := y + float64(g.config.CardPadding)

	// Warning header
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 20)
	dc.SetColor(g.colors.Warning)
	warningHeader := "费用告警"
	dc.DrawString(warningHeader, contentX, contentY+20)

	// Separator line
	contentY += 30
	g.drawHorizontalLineGG(dc, contentX, contentY, float64(g.config.Width-g.config.Padding*2-g.config.CardPadding*2))
	contentY += 15

	// Alert items
	dc.LoadFontFace("assets/fonts/SourceHanSansSC-Regular.otf", 16)
	dc.SetColor(g.colors.Danger)
	for i, alert := range alerts {
		alertText := fmt.Sprintf("• %s", alert)
		dc.DrawString(alertText, contentX+10, contentY+16)
		contentY += 35 // Consistent spacing between alerts
		
		if i < len(alerts)-1 {
			// No extra spacing needed, already handled by the 35px increment
		}
	}

	return y + cardHeight + float64(g.config.CardGap)
}

// Helper functions for gg

// drawCardGG draws a basic card using gg
func (g *ImageGenerator) drawCardGG(dc *gg.Context, x, y, width, height float64) {
	// Card background
	dc.SetColor(g.colors.CardBG)
	dc.DrawRectangle(x, y, width, height)
	dc.Fill()
	
	// Border
	dc.SetColor(g.colors.Border)
	dc.SetLineWidth(1)
	dc.DrawRectangle(x, y, width, height)
	dc.Stroke()
}

// drawWarningCardGG draws a warning card using gg
func (g *ImageGenerator) drawWarningCardGG(dc *gg.Context, x, y, width, height float64) {
	// Warning background
	warningBG := color.RGBA{255, 252, 240, 255}
	dc.SetColor(warningBG)
	dc.DrawRectangle(x, y, width, height)
	dc.Fill()
	
	// Warning border
	dc.SetColor(g.colors.Warning)
	dc.SetLineWidth(2)
	dc.DrawRectangle(x, y, width, height)
	dc.Stroke()
	
	// Left accent bar
	dc.SetColor(g.colors.Warning)
	dc.DrawRectangle(x+1, y, 3, height)
	dc.Fill()
}

// drawHorizontalLineGG draws a horizontal line using gg
func (g *ImageGenerator) drawHorizontalLineGG(dc *gg.Context, x, y, width float64) {
	dc.SetColor(g.colors.Border)
	dc.SetLineWidth(1)
	dc.DrawLine(x, y, x+width, y)
	dc.Stroke()
}

// encodePNGGG encodes gg context to PNG
func (g *ImageGenerator) encodePNGGG(dc *gg.Context) ([]byte, error) {
	img := dc.Image()
	
	var buf bytes.Buffer
	encoder := png.Encoder{
		CompressionLevel: png.BestCompression,
	}

	if err := encoder.Encode(&buf, img); err != nil {
		logger.Error("failed to encode PNG image", zap.Error(err))
		return nil, fmt.Errorf("failed to encode PNG image: %w", err)
	}

	data := buf.Bytes()
	
	// Size validation
	sizeMB := float64(len(data)) / 1024 / 1024
	logger.Info("generated PNG image", 
		zap.Float64("size_mb", sizeMB),
		zap.Int("width", g.config.Width),
		zap.Int("height", g.config.Height),
		zap.String("format", "PNG"))
		
	if sizeMB > g.config.MaxSizeMB {
		logger.Warn("PNG image exceeds size limit", 
			zap.Float64("actual_size_mb", sizeMB), 
			zap.Float64("limit_mb", g.config.MaxSizeMB),
			zap.String("recommendation", "consider reducing image dimensions or content"))
	}

	return data, nil
}