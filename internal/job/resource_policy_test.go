// This file tests resource-class policy and invalid classes.
package job

import "testing"

func TestResolveResources(t *testing.T) {
	tests := []struct {
		class     ResourceClass
		memoryMiB int64
	}{
		{ResourceClassSmall, 128},
		{ResourceClassMedium, 512},
		{ResourceClassLarge, 2048},
	}
	for _, test := range tests {
		resources, valid := ResolveResources(test.class)
		if !valid {
			t.Fatalf("class %q is invalid", test.class)
		}
		if resources.CPURequestMillis != 100 || resources.CPULimitMillis != 1000 ||
			resources.MemoryRequestBytes != test.memoryMiB*mebibyte ||
			resources.MemoryLimitBytes != test.memoryMiB*mebibyte {
			t.Errorf("class %q resources = %+v", test.class, resources)
		}
	}
	if _, valid := ResolveResources("unknown"); valid {
		t.Fatal("unknown class resolved successfully")
	}
}
