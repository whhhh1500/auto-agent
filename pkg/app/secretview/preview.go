// Package secretview provides bounded, non-length-revealing recognition views
// for explicitly designated secret fields.
package secretview

const fixedMask = "••••••••"

// Preview returns a fixed recognition view. Values with at least eight runes
// expose only their first three and last four runes; shorter values expose no
// source rune. The fixed mask never reflects the original length.
func Preview(value string) string {
	runes := []rune(value)
	if len(runes) < 8 {
		return fixedMask
	}
	return string(runes[:3]) + fixedMask + string(runes[len(runes)-4:])
}
