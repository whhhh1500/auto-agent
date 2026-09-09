// Package programmatic runs a deliberately small, deterministic program IR.
//
// The IR is intended for programmatic tool calling: a model can emit loops,
// branches, and data transformations, while the embedding host continues to
// own tool authorization and side effects. It is not a general-purpose
// scripting language and has no imports, filesystem, network, clock, random
// source, functions, recursion, exception handling, or concurrency.
//
// A source document has this shape:
//
//	{"version":"ptc-ir/v1","body":[
//	  {"op":"call","assign":"rows","tool":"catalog.search",
//	   "args":{"op":"map","entries":{"query":{"op":"literal","value":"boots"}}}},
//	  {"op":"assign","name":"active","value":{"op":"list","items":[]}},
//	  {"op":"for","var":"row","in":{"op":"var","name":"rows"},"body":[
//	   {"op":"if","cond":{"op":"cmp","kind":"eq",
//	    "left":{"op":"get","object":{"op":"var","name":"row"},"key":"active"},
//	    "right":{"op":"literal","value":true}},"then":[
//	      {"op":"append","target":"active","value":{"op":"var","name":"row"}}
//	   ]}
//	  ]},
//	  {"op":"if","cond":{"op":"cmp","kind":"gt",
//	   "left":{"op":"len","value":{"op":"var","name":"active"}},
//	   "right":{"op":"literal","value":0}},"then":[
//	    {"op":"call","assign":"detail","tool":"catalog.detail",
//	     "args":{"op":"map","entries":{"id":{"op":"get","object":{"op":"index","object":{"op":"var","name":"active"},"index":{"op":"literal","value":0}},"key":"id"}}}}
//	   ],"else":[{"op":"assign","name":"detail","value":{"op":"literal","value":null}}]},
//	  {"op":"return","value":{"op":"var","name":"detail"}}
//	 ]}
//
// Each Call includes a deterministic dynamic ordinal; the host owns capability
// binding validation and may use that ordinal to derive a durable child-call
// identity. The arena limit bounds values allocated and copied by this
// interpreter. It is not a process RSS or a security sandbox limit.
package programmatic
