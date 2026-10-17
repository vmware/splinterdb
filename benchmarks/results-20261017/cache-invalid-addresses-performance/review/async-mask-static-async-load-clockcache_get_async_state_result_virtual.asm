
.cache/invalid-address-perf-20261017/builds/async-load/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000004f470 <clockcache_get_async_state_result_virtual>:
   4f470:	mov    0x50(%rdi),%eax
   4f473:	test   %eax,%eax
   4f475:	jne    4f486 <clockcache_get_async_state_result_virtual+0x16>
   4f477:	mov    0x48(%rdi),%rax
   4f47b:	mov    %rax,(%rsi)
   4f47e:	mov    0x58(%rdi),%rax
   4f482:	mov    %rax,0x8(%rsi)
   4f486:	mov    0x50(%rdi),%eax
   4f489:	ret

Disassembly of section .fini:
