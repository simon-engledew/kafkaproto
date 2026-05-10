package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type RawSpec struct {
	Name             string      `json:"name"`
	Type             string      `json:"type"`
	APIKey           *int        `json:"apiKey"`
	ValidVersions    string      `json:"validVersions"`
	FlexibleVersions string      `json:"flexibleVersions"`
	Fields           []RawField  `json:"fields"`
	CommonStructs    []RawStruct `json:"commonStructs"`
}

type RawField struct {
	Name             string     `json:"name"`
	Type             string     `json:"type"`
	Versions         string     `json:"versions"`
	NullableVersions string     `json:"nullableVersions"`
	TaggedVersions   string     `json:"taggedVersions"`
	Tag              *int       `json:"tag"`
	Fields           []RawField `json:"fields"`
}

type RawStruct struct {
	Name     string     `json:"name"`
	Versions string     `json:"versions"`
	Fields   []RawField `json:"fields"`
}

// Spec is a fully-resolved spec ready to emit code from.
type Spec struct {
	Name      string // top-level name (e.g. "ProduceRequest")
	Type      string // request | response | header | data
	APIKey    int    // -1 if no key
	HasAPIKey bool

	Valid    VersionRange
	Flexible VersionRange

	Fields  []*Field           // top-level fields
	Structs map[string]*Struct // all named struct types referenced (incl. top-level + nested + commonStructs)
}

// Struct is a struct type to emit, identified by its source name.
// The Go type name is mapped through Spec.goTypeName.
type Struct struct {
	Name   string // source name (kafka spec)
	Fields []*Field
}

type Field struct {
	Name string

	// Resolved type tree.
	Kind FieldKind
	Elem *Field // for arrays only — represents one element

	// For Kind=KindStruct, the source name of the struct (key in Spec.Structs).
	StructName string

	Versions VersionRange
	Nullable VersionRange
	Tagged   VersionRange
	Tag      int
	HasTag   bool
}

type FieldKind int

const (
	KindBool FieldKind = iota
	KindInt8
	KindInt16
	KindUint16
	KindInt32
	KindUint32
	KindInt64
	KindFloat64
	KindString
	KindUUID
	KindBytes
	KindRecords
	KindArray
	KindStruct
)

var commentLine = regexp.MustCompile(`(?m)^[ \t]*//.*$`)

// stripComments removes leading-whitespace `//` line comments. Spec files only
// use this style, so we don't need a full JSON-with-comments parser.
func stripComments(src []byte) []byte {
	return commentLine.ReplaceAll(src, nil)
}

func ParseSpec(data []byte) (*Spec, error) {
	stripped := stripComments(data)
	var raw RawSpec
	if err := json.Unmarshal(stripped, &raw); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}

	valid, err := ParseVersionRange(raw.ValidVersions)
	if err != nil {
		return nil, fmt.Errorf("validVersions: %w", err)
	}
	flex, err := ParseVersionRange(raw.FlexibleVersions)
	if err != nil {
		return nil, fmt.Errorf("flexibleVersions: %w", err)
	}

	spec := &Spec{
		Name:     raw.Name,
		Type:     raw.Type,
		Valid:    valid,
		Flexible: flex,
		Structs:  map[string]*Struct{},
	}
	if raw.APIKey != nil {
		spec.APIKey = *raw.APIKey
		spec.HasAPIKey = true
	}

	// Pre-register commonStructs without resolving their fields (so cross-references work).
	for _, cs := range raw.CommonStructs {
		spec.Structs[cs.Name] = &Struct{Name: cs.Name}
	}

	// Resolve top-level fields. Inline-array struct definitions register new structs.
	tlFields, err := resolveFields(spec, raw.Fields)
	if err != nil {
		return nil, err
	}
	spec.Fields = tlFields

	// Resolve commonStructs after registration.
	for _, cs := range raw.CommonStructs {
		fs, err := resolveFields(spec, cs.Fields)
		if err != nil {
			return nil, fmt.Errorf("commonStruct %s: %w", cs.Name, err)
		}
		spec.Structs[cs.Name].Fields = fs
	}

	return spec, nil
}

func resolveFields(spec *Spec, raw []RawField) ([]*Field, error) {
	out := make([]*Field, 0, len(raw))
	for _, rf := range raw {
		f, err := resolveField(spec, rf)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", rf.Name, err)
		}
		out = append(out, f)
	}
	return out, nil
}

func resolveField(spec *Spec, rf RawField) (*Field, error) {
	versions, err := ParseVersionRange(rf.Versions)
	if err != nil {
		return nil, fmt.Errorf("versions: %w", err)
	}
	nullable, err := ParseVersionRange(rf.NullableVersions)
	if err != nil {
		return nil, fmt.Errorf("nullableVersions: %w", err)
	}
	tagged, err := ParseVersionRange(rf.TaggedVersions)
	if err != nil {
		return nil, fmt.Errorf("taggedVersions: %w", err)
	}

	f := &Field{
		Name:     rf.Name,
		Versions: versions,
		Nullable: nullable,
		Tagged:   tagged,
	}
	if rf.Tag != nil {
		f.Tag = *rf.Tag
		f.HasTag = true
	}

	if err := resolveType(spec, f, rf.Type, rf.Fields); err != nil {
		return nil, err
	}
	return f, nil
}

func resolveType(spec *Spec, f *Field, typ string, inlineFields []RawField) error {
	if strings.HasPrefix(typ, "[]") {
		f.Kind = KindArray
		// The element is implicitly in-range whenever its containing field is, and
		// individual elements are never themselves nullable or tagged — those flags
		// belong to the outer field. Initialize the per-aspect ranges to None so
		// the emitter doesn't read the zero-value VersionRange as "version 0 only".
		elem := &Field{
			Name:     f.Name + "$elem",
			Versions: f.Versions,
			Nullable: VersionRange{None: true},
			Tagged:   VersionRange{None: true},
		}
		if err := resolveType(spec, elem, typ[2:], inlineFields); err != nil {
			return err
		}
		f.Elem = elem
		return nil
	}
	switch typ {
	case "bool":
		f.Kind = KindBool
	case "int8":
		f.Kind = KindInt8
	case "int16":
		f.Kind = KindInt16
	case "uint16":
		f.Kind = KindUint16
	case "int32":
		f.Kind = KindInt32
	case "uint32":
		f.Kind = KindUint32
	case "int64":
		f.Kind = KindInt64
	case "float64":
		f.Kind = KindFloat64
	case "string":
		f.Kind = KindString
	case "uuid":
		f.Kind = KindUUID
	case "bytes":
		f.Kind = KindBytes
	case "records":
		f.Kind = KindRecords
	default:
		// Named struct. May be a commonStruct (already registered) or an inline
		// struct whose fields are provided here (defining the type for the first time).
		f.Kind = KindStruct
		f.StructName = typ
		if _, ok := spec.Structs[typ]; !ok {
			spec.Structs[typ] = &Struct{Name: typ}
		}
		if len(inlineFields) > 0 {
			fs, err := resolveFields(spec, inlineFields)
			if err != nil {
				return err
			}
			// Multiple references with field defs would conflict; the spec only
			// defines them once so we accept the first non-empty definition.
			if len(spec.Structs[typ].Fields) == 0 {
				spec.Structs[typ].Fields = fs
			}
		}
	}
	return nil
}
