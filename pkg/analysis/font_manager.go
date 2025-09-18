package analysis

import (
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
)

// FontManager manages font loading and caching using freetype-go
type FontManager struct {
	fontCache   map[string]*truetype.Font
	contextCache map[string]*freetype.Context
	cacheMutex  sync.RWMutex
	fallback    font.Face
}

// FontSize represents font size configuration
type FontSize int

const (
	FontSizeSmall  FontSize = 12
	FontSizeMedium FontSize = 14
	FontSizeLarge  FontSize = 16
	FontSizeXLarge FontSize = 18
	FontSizeTitle  FontSize = 20
)

var (
	globalFontManager *FontManager
	once              sync.Once
)

// GetFontManager returns the global font manager instance
func GetFontManager() *FontManager {
	once.Do(func() {
		globalFontManager = NewFontManager()
	})
	return globalFontManager
}

// NewFontManager creates a new font manager
func NewFontManager() *FontManager {
	fm := &FontManager{
		fontCache:    make(map[string]*truetype.Font),
		contextCache: make(map[string]*freetype.Context),
		fallback:     basicfont.Face7x13,
	}
	
	return fm
}

// GetFont returns a font face for the given size
func (fm *FontManager) GetFont(size FontSize) font.Face {
	cacheKey := fmt.Sprintf("chinese_%d", int(size))
	
	fm.cacheMutex.RLock()
	if ctx, exists := fm.contextCache[cacheKey]; exists {
		fm.cacheMutex.RUnlock()
		return &FreetypeFace{context: ctx, size: float64(size)}
	}
	fm.cacheMutex.RUnlock()
	
	// Load font if not cached
	font := fm.loadChineseFont()
	if font == nil {
		fmt.Printf("Failed to load Chinese font (size: %d), using fallback\n", int(size))
		return fm.fallback
	}
	
	// Create freetype context
	ctx := freetype.NewContext()
	ctx.SetDPI(96)
	ctx.SetFont(font)
	ctx.SetFontSize(float64(size))
	
	// Cache the context
	fm.cacheMutex.Lock()
	fm.contextCache[cacheKey] = ctx
	fm.cacheMutex.Unlock()
	
	return &FreetypeFace{context: ctx, size: float64(size)}
}

// loadChineseFont loads Chinese font with fallback strategy
func (fm *FontManager) loadChineseFont() *truetype.Font {
	// Try system fonts first (more reliable than OTF embedded fonts)
	if font := fm.trySystemFonts(); font != nil {
		return font
	}
	
	// Try embedded font as fallback (may fail with OTF format)
	if font := fm.tryEmbeddedFont(); font != nil {
		return font
	}
	
	return nil
}

// tryEmbeddedFont tries to load embedded Chinese font
func (fm *FontManager) tryEmbeddedFont() *truetype.Font {
	// Try multiple possible paths for the embedded font
	possiblePaths := []string{
		"assets/fonts/SourceHanSansSC-Regular.otf",
		"./assets/fonts/SourceHanSansSC-Regular.otf",
		"../assets/fonts/SourceHanSansSC-Regular.otf",
		"../../assets/fonts/SourceHanSansSC-Regular.otf",
		"../../../assets/fonts/SourceHanSansSC-Regular.otf",
	}
	
	// Also try to get path relative to executable
	if execPath, err := os.Executable(); err == nil {
		execDir := filepath.Dir(execPath)
		possiblePaths = append(possiblePaths, 
			filepath.Join(execDir, "assets/fonts/SourceHanSansSC-Regular.otf"),
			filepath.Join(filepath.Dir(execDir), "assets/fonts/SourceHanSansSC-Regular.otf"),
		)
	}
	
	for _, fontPath := range possiblePaths {
		// Check if file exists
		if _, err := os.Stat(fontPath); err != nil {
			continue // Try next path
		}
		
		font, err := fm.loadFontFromPath(fontPath)
		if err != nil {
			fmt.Printf("Failed to load embedded font from %s: %v\n", fontPath, err)
			continue // Try next path
		}
		
		// Successfully loaded
		fmt.Printf("Successfully loaded embedded Chinese font: %s\n", fontPath)
		return font
	}
	
	// No embedded font found
	fmt.Printf("Embedded font SourceHanSansSC-Regular.otf not found in any expected location\n")
	return nil
}

// trySystemFonts tries to load system Chinese fonts
func (fm *FontManager) trySystemFonts() *truetype.Font {
	fontPaths := fm.getSystemFontPaths()
	
	for _, fontPath := range fontPaths {
		if font, err := fm.loadFontFromPath(fontPath); err == nil && font != nil {
			fmt.Printf("Successfully loaded system Chinese font: %s\n", fontPath)
			return font
		}
	}
	
	return nil
}

// getSystemFontPaths returns system font paths for different OS
func (fm *FontManager) getSystemFontPaths() []string {
	var paths []string
	
	switch runtime.GOOS {
	case "darwin":
		// macOS Chinese fonts
		macFonts := []string{
			"/System/Library/Fonts/PingFang.ttc",
			"/System/Library/Fonts/STHeiti Light.ttc",
			"/System/Library/Fonts/STHeiti Medium.ttc",
			"/System/Library/Fonts/Helvetica.ttc",
			"/System/Library/Fonts/Arial.ttf",
			"/Library/Fonts/Arial.ttf",
		}
		paths = append(paths, macFonts...)
		
	case "linux":
		// Linux Chinese fonts
		linuxFonts := []string{
			"/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
			"/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
			"/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
			"/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
			"/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
			"/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
		}
		paths = append(paths, linuxFonts...)
		
	case "windows":
		// Windows Chinese fonts
		winFonts := "C:\\Windows\\Fonts"
		windowsFonts := []string{
			filepath.Join(winFonts, "simsun.ttc"),
			filepath.Join(winFonts, "simhei.ttf"),
			filepath.Join(winFonts, "msyh.ttc"),
			filepath.Join(winFonts, "arial.ttf"),
			filepath.Join(winFonts, "calibri.ttf"),
		}
		paths = append(paths, windowsFonts...)
	}
	
	return paths
}

// loadFontFromPath loads font from file path using freetype-go
func (fm *FontManager) loadFontFromPath(fontPath string) (*truetype.Font, error) {
	if _, err := os.Stat(fontPath); err != nil {
		return nil, fmt.Errorf("font file not found: %s", fontPath)
	}
	
	fontData, err := ioutil.ReadFile(fontPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read font file: %w", err)
	}
	
	// Parse font using truetype package (supports both TTF and OTF)
	font, err := truetype.Parse(fontData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse font: %w", err)
	}
	
	return font, nil
}

// ClearCache clears the font cache
func (fm *FontManager) ClearCache() {
	fm.cacheMutex.Lock()
	defer fm.cacheMutex.Unlock()
	
	oldSize := len(fm.fontCache)
	fm.fontCache = make(map[string]*truetype.Font)
	fm.contextCache = make(map[string]*freetype.Context)
	
	fmt.Printf("Font cache cleared (previously had %d cached fonts)\n", oldSize)
}

// GetCacheStats returns cache statistics
func (fm *FontManager) GetCacheStats() map[string]interface{} {
	fm.cacheMutex.RLock()
	defer fm.cacheMutex.RUnlock()
	
	return map[string]interface{}{
		"cached_fonts": len(fm.fontCache),
		"cached_contexts": len(fm.contextCache),
		"fallback": "basicfont.Face7x13",
	}
}

// FreetypeFace implements font.Face using freetype context
type FreetypeFace struct {
	context *freetype.Context
	size    float64
}

// Close implements font.Face
func (f *FreetypeFace) Close() error {
	return nil
}

// Glyph implements font.Face
func (f *FreetypeFace) Glyph(dot fixed.Point26_6, r rune) (dr image.Rectangle, mask image.Image, maskp image.Point, advance fixed.Int26_6, ok bool) {
	// This is a simplified implementation for basic text rendering
	// For full glyph support, you would need more complex implementation
	return image.Rectangle{}, nil, image.Point{}, fixed.I(int(f.size)), true
}

// GlyphBounds implements font.Face
func (f *FreetypeFace) GlyphBounds(r rune) (bounds fixed.Rectangle26_6, advance fixed.Int26_6, ok bool) {
	return fixed.Rectangle26_6{}, fixed.I(int(f.size)), true
}

// GlyphAdvance implements font.Face
func (f *FreetypeFace) GlyphAdvance(r rune) (advance fixed.Int26_6, ok bool) {
	return fixed.I(int(f.size)), true
}

// Kern implements font.Face
func (f *FreetypeFace) Kern(r0, r1 rune) fixed.Int26_6 {
	return 0
}

// Metrics implements font.Face
func (f *FreetypeFace) Metrics() font.Metrics {
	return font.Metrics{
		Height:     fixed.I(int(f.size * 1.2)),
		Ascent:     fixed.I(int(f.size * 0.8)),
		Descent:    fixed.I(int(f.size * 0.2)),
		XHeight:    fixed.I(int(f.size * 0.5)),
		CapHeight:  fixed.I(int(f.size * 0.7)),
		CaretSlope: image.Point{X: 0, Y: 1},
	}
}

// DrawText draws text using freetype context
func (f *FreetypeFace) DrawText(dst draw.Image, text string, x, y int, c color.Color) error {
	f.context.SetDst(dst)
	f.context.SetSrc(image.NewUniform(c))
	f.context.SetClip(dst.Bounds())
	
	pt := freetype.Pt(x, y+int(f.size))
	_, err := f.context.DrawString(text, pt)
	return err
}