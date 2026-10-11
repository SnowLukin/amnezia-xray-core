#include "textflag.h"

// Both supported Darwin architectures use the same libSystem trampoline.
TEXT quality_libc_getsockopt_trampoline<>(SB),NOSPLIT,$0-0
	JMP quality_libc_getsockopt(SB)
GLOBL ·qualityLibcGetsockoptTrampoline(SB), RODATA, $8
DATA ·qualityLibcGetsockoptTrampoline(SB)/8, $quality_libc_getsockopt_trampoline<>(SB)
