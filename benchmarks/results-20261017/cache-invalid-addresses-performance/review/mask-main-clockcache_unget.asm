
.cache/invalid-address-perf-20261017/builds/mask-main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000529a0 <clockcache_unget>:
   529a0:	mov    0x48(%rdi),%rdx
   529a4:	mov    (%rsi),%rax
   529a7:	mov    %rdi,%rcx
   529aa:	mov    0x8(%rsi),%rdi
   529ae:	sub    %rdx,%rax
   529b1:	sar    $0x6,%rax
   529b5:	mov    %eax,%eax
   529b7:	mov    %rax,%rsi
   529ba:	shl    $0x6,%rsi
   529be:	mov    0x10(%rdx,%rsi,1),%edx
   529c2:	and    $0x2,%edx
   529c5:	je     52a10 <clockcache_unget+0x70>
   529c7:	mov    0x8(%rcx),%rsi
   529cb:	xor    %edx,%edx
   529cd:	mov    %rdi,%r9
   529d0:	and    $0x3,%r9d
   529d4:	divq   0x140(%rsi)
   529db:	shl    $0x6,%edx
   529de:	lea    (%rdx,%rax,1),%r8d
   529e2:	mov    0x130(%rsi),%eax
   529e8:	mov    0x88(%rcx),%rdx
   529ef:	mov    $0xffffffff,%ecx
   529f4:	imul   %r9,%rax
   529f8:	add    %r8,%rax
   529fb:	lock xadd %cx,(%rdx,%rax,2)
   52a01:	test   %cx,%cx
   52a04:	je     52a1c <clockcache_unget+0x7c>
   52a06:	ret
   52a07:	nopw   0x0(%rax,%rax,1)
   52a10:	mov    0x48(%rcx),%rdx
   52a14:	lock orl $0x2,0x10(%rdx,%rsi,1)
   52a1a:	jmp    529c7 <clockcache_unget+0x27>
   52a1c:	sub    $0x10,%rsp
   52a20:	lea    0x5ebfd(%rip),%rcx        # b1624 <_fini+0xa08>
   52a27:	lea    0x6ff82(%rip),%rdx        # c29b0 <__func__.28.lto_priv.1>
   52a2e:	xor    %eax,%eax
   52a30:	push   %r9
   52a32:	mov    $0x261,%esi
   52a37:	xor    %r9d,%r9d
   52a3a:	push   %r8
   52a3c:	lea    0x657bd(%rip),%r8        # b8200 <__func__.21+0xd30>
   52a43:	push   %rdi
   52a44:	lea    0x5eb8c(%rip),%rdi        # b15d7 <_fini+0x9bb>
   52a4b:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
