
.cache/invalid-address-perf-20261017/builds/mask-main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000aa320 <clockcache_try_get_read.constprop.0>:
   aa320:	push   %rbp
   aa321:	mov    %esi,%ebp
   aa323:	push   %rbx
   aa324:	mov    %rdi,%rbx
   aa327:	sub    $0x8,%rsp
   aa32b:	data16 lea 0x2ac75(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   aa333:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   aa33b:	mov    0x48(%rbx),%r10
   aa33f:	mov    %ebp,%ecx
   aa341:	mov    %rcx,%r9
   aa344:	shl    $0x6,%r9
   aa348:	add    %r9,%r10
   aa34b:	mov    (%rax),%rdi
   aa34e:	mov    0x10(%r10),%eax
   aa352:	test   $0x20,%al
   aa354:	jne    aa3c8 <clockcache_try_get_read.constprop.0+0xa8>
   aa356:	mov    0x8(%rbx),%rsi
   aa35a:	mov    %rcx,%rax
   aa35d:	xor    %edx,%edx
   aa35f:	mov    %rdi,%r8
   aa362:	and    $0x3,%r8d
   aa366:	divq   0x140(%rsi)
   aa36d:	shl    $0x6,%edx
   aa370:	add    %eax,%edx
   aa372:	mov    0x130(%rsi),%eax
   aa378:	mov    %edx,%edx
   aa37a:	imul   %r8,%rax
   aa37e:	add    %rax,%rdx
   aa381:	mov    0x88(%rbx),%rax
   aa388:	lock addw $0x1,(%rax,%rdx,2)
   aa38e:	je     aa41e <clockcache_try_get_read.constprop.0+0xfe>
   aa394:	mov    0x10(%r10),%esi
   aa398:	mov    %esi,%eax
   aa39a:	and    $0x21,%eax
   aa39d:	jne    aa3d8 <clockcache_try_get_read.constprop.0+0xb8>
   aa39f:	and    $0x2,%esi
   aa3a2:	je     aa3b0 <clockcache_try_get_read.constprop.0+0x90>
   aa3a4:	add    $0x8,%rsp
   aa3a8:	pop    %rbx
   aa3a9:	pop    %rbp
   aa3aa:	ret
   aa3ab:	nopl   0x0(%rax,%rax,1)
   aa3b0:	mov    0x48(%rbx),%rdx
   aa3b4:	lock orl $0x2,0x10(%rdx,%r9,1)
   aa3bb:	add    $0x8,%rsp
   aa3bf:	pop    %rbx
   aa3c0:	pop    %rbp
   aa3c1:	ret
   aa3c2:	nopw   0x0(%rax,%rax,1)
   aa3c8:	add    $0x8,%rsp
   aa3cc:	mov    $0x1,%eax
   aa3d1:	pop    %rbx
   aa3d2:	pop    %rbp
   aa3d3:	ret
   aa3d4:	nopl   0x0(%rax)
   aa3d8:	mov    0x8(%rbx),%r9
   aa3dc:	mov    %rcx,%rax
   aa3df:	xor    %edx,%edx
   aa3e1:	divq   0x140(%r9)
   aa3e8:	shl    $0x6,%edx
   aa3eb:	lea    (%rdx,%rax,1),%ecx
   aa3ee:	mov    0x130(%r9),%eax
   aa3f5:	mov    $0xffffffff,%r9d
   aa3fb:	mov    0x88(%rbx),%rdx
   aa402:	imul   %r8,%rax
   aa406:	add    %rcx,%rax
   aa409:	lock xadd %r9w,(%rdx,%rax,2)
   aa410:	test   %r9w,%r9w
   aa414:	je     aa44c <clockcache_try_get_read.constprop.0+0x12c>
   aa416:	and    $0x1,%esi
   aa419:	lea    0x1(%rsi),%eax
   aa41c:	jmp    aa3a4 <clockcache_try_get_read.constprop.0+0x84>
   aa41e:	push   %rdx
   aa41f:	mov    %ebp,%r9d
   aa422:	lea    0x71dd(%rip),%rcx        # b1606 <_fini+0x9ea>
   aa429:	mov    $0x243,%esi
   aa42e:	push   %r8
   aa430:	lea    0x18539(%rip),%rdx        # c2970 <__func__.29.lto_priv.1>
   aa437:	lea    0xdd8a(%rip),%r8        # b81c8 <__func__.21+0xcf8>
   aa43e:	xor    %eax,%eax
   aa440:	lea    0x7190(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa447:	call   ee00 <platform_assert_false@plt>
   aa44c:	push   %rax
   aa44d:	xor    %r9d,%r9d
   aa450:	lea    0x18559(%rip),%rdx        # c29b0 <__func__.28.lto_priv.1>
   aa457:	mov    $0x261,%esi
   aa45c:	push   %r8
   aa45e:	xor    %eax,%eax
   aa460:	lea    0xdd99(%rip),%r8        # b8200 <__func__.21+0xd30>
   aa467:	push   %rcx
   aa468:	lea    0x71b5(%rip),%rcx        # b1624 <_fini+0xa08>
   aa46f:	push   %rdi
   aa470:	lea    0x7160(%rip),%rdi        # b15d7 <_fini+0x9bb>
   aa477:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
