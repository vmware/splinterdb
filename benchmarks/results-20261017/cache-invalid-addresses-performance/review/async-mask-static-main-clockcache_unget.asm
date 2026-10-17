
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000529d0 <clockcache_unget>:
   529d0:	mov    0x48(%rdi),%rdx
   529d4:	mov    (%rsi),%rax
   529d7:	mov    %rdi,%rcx
   529da:	mov    0x8(%rsi),%rdi
   529de:	sub    %rdx,%rax
   529e1:	sar    $0x6,%rax
   529e5:	mov    %eax,%eax
   529e7:	mov    %rax,%rsi
   529ea:	shl    $0x6,%rsi
   529ee:	mov    0x10(%rdx,%rsi,1),%edx
   529f2:	and    $0x2,%edx
   529f5:	je     52a40 <clockcache_unget+0x70>
   529f7:	mov    0x8(%rcx),%rsi
   529fb:	xor    %edx,%edx
   529fd:	mov    %rdi,%r9
   52a00:	and    $0x3,%r9d
   52a04:	divq   0x140(%rsi)
   52a0b:	shl    $0x6,%edx
   52a0e:	lea    (%rdx,%rax,1),%r8d
   52a12:	mov    0x130(%rsi),%eax
   52a18:	mov    0x88(%rcx),%rdx
   52a1f:	mov    $0xffffffff,%ecx
   52a24:	imul   %r9,%rax
   52a28:	add    %r8,%rax
   52a2b:	lock xadd %cx,(%rdx,%rax,2)
   52a31:	test   %cx,%cx
   52a34:	je     52a4c <clockcache_unget+0x7c>
   52a36:	ret
   52a37:	nopw   0x0(%rax,%rax,1)
   52a40:	mov    0x48(%rcx),%rdx
   52a44:	lock orl $0x2,0x10(%rdx,%rsi,1)
   52a4a:	jmp    529f7 <clockcache_unget+0x27>
   52a4c:	sub    $0x10,%rsp
   52a50:	lea    0x5ebcd(%rip),%rcx        # b1624 <_fini+0xa48>
   52a57:	lea    0x6ff32(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   52a5e:	xor    %eax,%eax
   52a60:	push   %r9
   52a62:	mov    $0x261,%esi
   52a67:	xor    %r9d,%r9d
   52a6a:	push   %r8
   52a6c:	lea    0x6577d(%rip),%r8        # b81f0 <__func__.21+0xd30>
   52a73:	push   %rdi
   52a74:	lea    0x5eb5c(%rip),%rdi        # b15d7 <_fini+0x9fb>
   52a7b:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
