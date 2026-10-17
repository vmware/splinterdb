
.cache/invalid-address-perf-20261017/builds/async-mask/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa3e0 <clockcache_try_get_read.constprop.0>:
   aa3e0:	push   %rbp
   aa3e1:	mov    %esi,%ebp
   aa3e3:	push   %rbx
   aa3e4:	mov    %rdi,%rbx
   aa3e7:	sub    $0x8,%rsp
   aa3eb:	data16 lea 0x2abb5(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   aa3f3:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa3fb:	mov    0x48(%rbx),%r10
   aa3ff:	mov    %ebp,%ecx
   aa401:	mov    %rcx,%r9
   aa404:	shl    $0x6,%r9
   aa408:	add    %r9,%r10
   aa40b:	mov    (%rax),%rdi
   aa40e:	mov    0x10(%r10),%eax
   aa412:	test   $0x20,%al
   aa414:	jne    aa488 <clockcache_try_get_read.constprop.0+0xa8>
   aa416:	mov    0x8(%rbx),%rsi
   aa41a:	mov    %rcx,%rax
   aa41d:	xor    %edx,%edx
   aa41f:	mov    %rdi,%r8
   aa422:	and    $0x3,%r8d
   aa426:	divq   0x140(%rsi)
   aa42d:	shl    $0x6,%edx
   aa430:	add    %eax,%edx
   aa432:	mov    0x130(%rsi),%eax
   aa438:	mov    %edx,%edx
   aa43a:	imul   %r8,%rax
   aa43e:	add    %rax,%rdx
   aa441:	mov    0x88(%rbx),%rax
   aa448:	lock addw $0x1,(%rax,%rdx,2)
   aa44e:	je     aa4de <clockcache_try_get_read.constprop.0+0xfe>
   aa454:	mov    0x10(%r10),%esi
   aa458:	mov    %esi,%eax
   aa45a:	and    $0x21,%eax
   aa45d:	jne    aa498 <clockcache_try_get_read.constprop.0+0xb8>
   aa45f:	and    $0x2,%esi
   aa462:	je     aa470 <clockcache_try_get_read.constprop.0+0x90>
   aa464:	add    $0x8,%rsp
   aa468:	pop    %rbx
   aa469:	pop    %rbp
   aa46a:	ret
   aa46b:	nopl   0x0(%rax,%rax,1)
   aa470:	mov    0x48(%rbx),%rdx
   aa474:	lock orl $0x2,0x10(%rdx,%r9,1)
   aa47b:	add    $0x8,%rsp
   aa47f:	pop    %rbx
   aa480:	pop    %rbp
   aa481:	ret
   aa482:	nopw   0x0(%rax,%rax,1)
   aa488:	add    $0x8,%rsp
   aa48c:	mov    $0x1,%eax
   aa491:	pop    %rbx
   aa492:	pop    %rbp
   aa493:	ret
   aa494:	nopl   0x0(%rax)
   aa498:	mov    0x8(%rbx),%r9
   aa49c:	mov    %rcx,%rax
   aa49f:	xor    %edx,%edx
   aa4a1:	divq   0x140(%r9)
   aa4a8:	shl    $0x6,%edx
   aa4ab:	lea    (%rdx,%rax,1),%ecx
   aa4ae:	mov    0x130(%r9),%eax
   aa4b5:	mov    $0xffffffff,%r9d
   aa4bb:	mov    0x88(%rbx),%rdx
   aa4c2:	imul   %r8,%rax
   aa4c6:	add    %rcx,%rax
   aa4c9:	lock xadd %r9w,(%rdx,%rax,2)
   aa4d0:	test   %r9w,%r9w
   aa4d4:	je     aa50c <clockcache_try_get_read.constprop.0+0x12c>
   aa4d6:	and    $0x1,%esi
   aa4d9:	lea    0x1(%rsi),%eax
   aa4dc:	jmp    aa464 <clockcache_try_get_read.constprop.0+0x84>
   aa4de:	push   %rdx
   aa4df:	mov    %ebp,%r9d
   aa4e2:	lea    0x718a(%rip),%rcx        # b1673 <_fini+0xa57>
   aa4e9:	mov    $0x26a,%esi
   aa4ee:	push   %r8
   aa4f0:	lea    0x18519(%rip),%rdx        # c2a10 <__func__.30.lto_priv.1>
   aa4f7:	lea    0xdf9a(%rip),%r8        # b8498 <__func__.21+0x1008>
   aa4fe:	xor    %eax,%eax
   aa500:	lea    0x70d0(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa507:	call   ee00 <platform_assert_false@plt>
   aa50c:	push   %rax
   aa50d:	xor    %r9d,%r9d
   aa510:	lea    0x18519(%rip),%rdx        # c2a30 <__func__.28.lto_priv.1>
   aa517:	mov    $0x288,%esi
   aa51c:	push   %r8
   aa51e:	xor    %eax,%eax
   aa520:	lea    0xdcd1(%rip),%r8        # b81f8 <__func__.21+0xd68>
   aa527:	push   %rcx
   aa528:	lea    0x70ff(%rip),%rcx        # b162e <_fini+0xa12>
   aa52f:	push   %rdi
   aa530:	lea    0x70a0(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa537:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
