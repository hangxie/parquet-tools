package cmd

// CommonOption represents common options across most commands
type CommonOption struct {
	URI   string `arg help:"URI of Parquet file, support s3:// and file://."`
	Debug bool   `help:"Output debug information."`
}

// Context represents command's context
type Context struct {
	Version string
	Build   string
}
