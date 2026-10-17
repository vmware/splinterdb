
.cache/invalid-address-perf-20261017/builds/async-load/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000512e0 <clockcache_unget>:
   512e0:	mov    0x48(%rdi),%rdx
   512e4:	mov    (%rsi),%rax
   512e7:	mov    %rdi,%rcx
   512ea:	mov    0x8(%rsi),%rdi
   512ee:	sub    %rdx,%rax
   512f1:	sar    $0x6,%rax
   512f5:	mov    %eax,%eax
   512f7:	mov    %rax,%rsi
   512fa:	shl    $0x6,%rsi
   512fe:	mov    0x10(%rdx,%rsi,1),%edx
   51302:	and    $0x2,%edx
   51305:	je     51350 <clockcache_unget+0x70>
   51307:	mov    0x8(%rcx),%rsi
   5130b:	xor    %edx,%edx
   5130d:	mov    %rdi,%r9
   51310:	and    $0x3,%r9d
   51314:	divq   0x140(%rsi)
   5131b:	shl    $0x6,%edx
   5131e:	lea    (%rdx,%rax,1),%r8d
   51322:	mov    0x130(%rsi),%eax
   51328:	mov    0x88(%rcx),%rdx
   5132f:	mov    $0xffffffff,%ecx
   51334:	imul   %r9,%rax
   51338:	add    %r8,%rax
   5133b:	lock xadd %cx,(%rdx,%rax,2)
   51341:	test   %cx,%cx
   51344:	je     5135c <clockcache_unget+0x7c>
   51346:	ret
   51347:	nopw   0x0(%rax,%rax,1)
   51350:	mov    0x48(%rcx),%rdx
   51354:	lock orl $0x2,0x10(%rdx,%rsi,1)
   5135a:	jmp    51307 <clockcache_unget+0x27>
   5135c:	sub    $0x10,%rsp
   51360:	lea    0x602c7(%rip),%rcx        # b162e <_fini+0xa12>
   51367:	lea    0x716a2(%rip),%rdx        # c2a10 <__func__.28.lto_priv.1>
   5136e:	xor    %eax,%eax
   51370:	push   %r9
   51372:	mov    $0x288,%esi
   51377:	xor    %r9d,%r9d
   5137a:	push   %r8
   5137c:	lea    0x66e75(%rip),%r8        # b81f8 <__func__.21+0xd68>
   51383:	push   %rdi
   51384:	lea    0x6024c(%rip),%rdi        # b15d7 <_fini+0x9bb>
   5138b:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
