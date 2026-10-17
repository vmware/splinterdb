
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000004f170 <clockcache_get_async_state_result_virtual>:
   4f170:	mov    0x50(%rdi),%eax
   4f173:	test   %eax,%eax
   4f175:	jne    4f186 <clockcache_get_async_state_result_virtual+0x16>
   4f177:	mov    0x48(%rdi),%rax
   4f17b:	mov    %rax,(%rsi)
   4f17e:	mov    0x58(%rdi),%rax
   4f182:	mov    %rax,0x8(%rsi)
   4f186:	mov    0x50(%rdi),%eax
   4f189:	ret

Disassembly of section .fini:
