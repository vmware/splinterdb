
.cache/invalid-address-perf-20261017/builds/async-load/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009c750 <clockcache_get_from_disk_async.constprop.0>:
   9c750:	mov    0x18(%rdi),%rax
   9c754:	cmp    $0x1,%rax
   9c758:	je     9ca50 <clockcache_get_from_disk_async.constprop.0+0x300>
   9c75e:	push   %r13
   9c760:	push   %r12
   9c762:	push   %rbp
   9c763:	push   %rbx
   9c764:	mov    %rdi,%rbx
   9c767:	sub    $0x18,%rsp
   9c76b:	test   %rax,%rax
   9c76e:	je     9c890 <clockcache_get_from_disk_async.constprop.0+0x140>
   9c774:	lea    0x80(%rdi),%rbp
   9c77b:	mov    0x80(%rbx),%rax
   9c782:	mov    %rbp,%rdi
   9c785:	call   *0x8(%rax)
   9c788:	cmp    $0x1,%eax
   9c78b:	jne    9c818 <clockcache_get_from_disk_async.constprop.0+0xc8>
   9c791:	mov    0x80(%rbx),%rax
   9c798:	mov    %rbp,%rdi
   9c79b:	call   *0x10(%rax)
   9c79e:	mov    0x20(%rbx),%rdx
   9c7a2:	mov    %eax,0x70(%rbx)
   9c7a5:	mov    0x60(%rbx),%eax
   9c7a8:	shl    $0x6,%rax
   9c7ac:	add    0x48(%rdx),%rax
   9c7b0:	mov    0x10(%rax),%eax
   9c7b3:	test   $0x10,%al
   9c7b5:	jne    9ca30 <clockcache_get_from_disk_async.constprop.0+0x2e0>
   9c7bb:	mov    0x70(%rbx),%edi
   9c7be:	test   %edi,%edi
   9c7c0:	jne    9c9f0 <clockcache_get_from_disk_async.constprop.0+0x2a0>
   9c7c6:	mov    0x80(%rbx),%rax
   9c7cd:	mov    %rbp,%rdi
   9c7d0:	call   *0x20(%rax)
   9c7d3:	mov    0x20(%rbx),%rsi
   9c7d7:	mov    0x70(%rbx),%edi
   9c7da:	mov    0x8(%rsi),%rcx
   9c7de:	test   %edi,%edi
   9c7e0:	jne    9ca60 <clockcache_get_from_disk_async.constprop.0+0x310>
   9c7e6:	mov    0x18(%rcx),%eax
   9c7e9:	test   %eax,%eax
   9c7eb:	jne    9c828 <clockcache_get_from_disk_async.constprop.0+0xd8>
   9c7ed:	mov    0x68(%rbx),%rax
   9c7f1:	mov    %rax,0x48(%rbx)
   9c7f5:	movl   $0x1,0x54(%rbx)
   9c7fc:	movq   $0x1,0x18(%rbx)
   9c804:	add    $0x18,%rsp
   9c808:	mov    $0x1,%eax
   9c80d:	pop    %rbx
   9c80e:	pop    %rbp
   9c80f:	pop    %r12
   9c811:	pop    %r13
   9c813:	ret
   9c814:	nopl   0x0(%rax)
   9c818:	add    $0x18,%rsp
   9c81c:	xor    %eax,%eax
   9c81e:	pop    %rbx
   9c81f:	pop    %rbp
   9c820:	pop    %r12
   9c822:	pop    %r13
   9c824:	ret
   9c825:	nopl   (%rax)
   9c828:	mov    %rsp,%rsi
   9c82b:	mov    $0x1,%edi
   9c830:	mov    0x480(%rbx),%rbp
   9c837:	call   e410 <clock_gettime@plt>
   9c83c:	mov    0x58(%rbx),%rcx
   9c840:	imul   $0x3b9aca00,(%rsp),%rdx
   9c848:	add    0x8(%rsp),%rdx
   9c84d:	lea    0x0(,%rcx,8),%rax
   9c855:	sub    %rbp,%rdx
   9c858:	sub    %rcx,%rax
   9c85b:	mov    0x30(%rbx),%ecx
   9c85e:	lea    (%rcx,%rax,8),%rcx
   9c862:	mov    0x20(%rbx),%rax
   9c866:	lea    (%rax,%rcx,8),%rax
   9c86a:	addq   $0x1,0x1148(%rax)
   9c872:	addq   $0x1,0x1220(%rax)
   9c87a:	add    %rdx,0x1190(%rax)
   9c881:	jmp    9c7ed <clockcache_get_from_disk_async.constprop.0+0x9d>
   9c886:	cs nopw 0x0(%rax,%rax,1)
   9c890:	mov    0x20(%rdi),%rax
   9c894:	mov    0x28(%rdi),%rbp
   9c898:	mov    0x10(%rax),%r12
   9c89c:	mov    (%r12),%rax
   9c8a0:	mov    %r12,%rdi
   9c8a3:	call   *(%rax)
   9c8a5:	xor    %edx,%edx
   9c8a7:	mov    %rax,%rcx
   9c8aa:	mov    (%rax),%rax
   9c8ad:	mov    0x8(%rax),%r9
   9c8b1:	mov    %rbp,%rax
   9c8b4:	div    %r9
   9c8b7:	test   %rdx,%rdx
   9c8ba:	jne    9cae0 <clockcache_get_from_disk_async.constprop.0+0x390>
   9c8c0:	mov    (%r12),%rax
   9c8c4:	mov    %rbp,%r13
   9c8c7:	and    0x20(%rcx),%r13
   9c8cb:	je     9c990 <clockcache_get_from_disk_async.constprop.0+0x240>
   9c8d1:	cmp    0x8(%rcx),%rbp
   9c8d5:	jae    9c990 <clockcache_get_from_disk_async.constprop.0+0x240>
   9c8db:	mov    %r13,%rsi
   9c8de:	mov    %r12,%rdi
   9c8e1:	call   *0x20(%rax)
   9c8e4:	test   %eax,%eax
   9c8e6:	je     9cbe8 <clockcache_get_from_disk_async.constprop.0+0x498>
   9c8ec:	mov    0x30(%rbx),%edx
   9c8ef:	mov    0x28(%rbx),%rsi
   9c8f3:	mov    0x20(%rbx),%rdi
   9c8f7:	call   536c0 <clockcache_acquire_entry_for_load.lto_priv.0>
   9c8fc:	mov    $0xffffffff,%edx
   9c901:	mov    %rax,0x60(%rbx)
   9c905:	cmp    %rdx,%rax
   9c908:	je     9cbd0 <clockcache_get_from_disk_async.constprop.0+0x480>
   9c90e:	mov    0x20(%rbx),%rdx
   9c912:	shl    $0x6,%rax
   9c916:	mov    0x28(%rbx),%rcx
   9c91a:	mov    %rbx,%r9
   9c91d:	lea    0x80(%rbx),%rbp
   9c924:	mov    0x38605(%rip),%r8        # d4f30 <clockcache_get_from_disk_async_callback@@Base+0x80e70>
   9c92b:	mov    0x18(%rdx),%rsi
   9c92f:	add    0x48(%rdx),%rax
   9c933:	mov    %rbp,%rdi
   9c936:	xor    %edx,%edx
   9c938:	mov    %rax,0x68(%rbx)
   9c93c:	mov    (%rsi),%rax
   9c93f:	call   *0x18(%rax)
   9c942:	mov    %eax,0x70(%rbx)
   9c945:	test   %eax,%eax
   9c947:	jne    9cc30 <clockcache_get_from_disk_async.constprop.0+0x4e0>
   9c94d:	mov    0x68(%rbx),%rax
   9c951:	mov    %rbp,%rdi
   9c954:	mov    (%rax),%rsi
   9c957:	mov    0x80(%rbx),%rax
   9c95e:	call   *(%rax)
   9c960:	mov    %eax,0x70(%rbx)
   9c963:	test   %eax,%eax
   9c965:	jne    9cb20 <clockcache_get_from_disk_async.constprop.0+0x3d0>
   9c96b:	mov    0x20(%rbx),%rax
   9c96f:	mov    0x8(%rax),%rax
   9c973:	mov    0x18(%rax),%r8d
   9c977:	test   %r8d,%r8d
   9c97a:	jne    9ccd0 <clockcache_get_from_disk_async.constprop.0+0x580>
   9c980:	lea    -0x20c(%rip),%rax        # 9c77b <clockcache_get_from_disk_async.constprop.0+0x2b>
   9c987:	mov    %rax,0x18(%rbx)
   9c98b:	jmp    9c77b <clockcache_get_from_disk_async.constprop.0+0x2b>
   9c990:	mov    %r12,%rdi
   9c993:	call   *0x58(%rax)
   9c996:	mov    0x38633(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9c99d:	mov    %rbp,%r9
   9c9a0:	mov    %r13,%r8
   9c9a3:	sub    $0x8,%rsp
   9c9a7:	mov    $0x17d,%ecx
   9c9ac:	lea    0x25f1d(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   9c9b3:	mov    (%r12),%rdi
   9c9b7:	push   %rax
   9c9b8:	lea    0x1aba1(%rip),%rsi        # b7560 <__func__.21+0xd0>
   9c9bf:	xor    %eax,%eax
   9c9c1:	call   fbf0 <fprintf@plt>
   9c9c6:	mov    (%r12),%rdi
   9c9ca:	call   fe90 <fflush@plt>
   9c9cf:	pop    %rbp
   9c9d0:	pop    %r12
   9c9d2:	mov    0x261e7(%rip),%rax        # c2bc0 <__func__.0.lto_priv.4+0x60>
   9c9d9:	movq   $0x1,0x18(%rbx)
   9c9e1:	mov    %rax,0x50(%rbx)
   9c9e5:	jmp    9c804 <clockcache_get_from_disk_async.constprop.0+0xb4>
   9c9ea:	nopw   0x0(%rax,%rax,1)
   9c9f0:	call   efe0 <strerror@plt>
   9c9f5:	mov    0x385d4(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9c9fc:	mov    0x60(%rbx),%rcx
   9ca00:	lea    0x240e9(%rip),%rsi        # c0af0 <__func__.21+0x9660>
   9ca07:	mov    0x28(%rbx),%rdx
   9ca0b:	mov    %rax,%r8
   9ca0e:	xor    %eax,%eax
   9ca10:	mov    (%r12),%rdi
   9ca14:	call   fbf0 <fprintf@plt>
   9ca19:	mov    (%r12),%rdi
   9ca1d:	call   fe90 <fflush@plt>
   9ca22:	jmp    9c7c6 <clockcache_get_from_disk_async.constprop.0+0x76>
   9ca27:	nopw   0x0(%rax,%rax,1)
   9ca30:	mov    0x70(%rbx),%ecx
   9ca33:	mov    0x28(%rbx),%rsi
   9ca37:	mov    0x20(%rbx),%rdi
   9ca3b:	mov    0x60(%rbx),%edx
   9ca3e:	call   537f0 <clockcache_finish_load.lto_priv.0>
   9ca43:	jmp    9c7bb <clockcache_get_from_disk_async.constprop.0+0x6b>
   9ca48:	nopl   0x0(%rax,%rax,1)
   9ca50:	mov    $0x1,%eax
   9ca55:	ret
   9ca56:	cs nopw 0x0(%rax,%rax,1)
   9ca60:	mov    0x70(%rbx),%eax
   9ca63:	xor    %edx,%edx
   9ca65:	mov    0x58(%rbx),%rdi
   9ca69:	mov    %eax,0x50(%rbx)
   9ca6c:	mov    0x60(%rbx),%eax
   9ca6f:	mov    %rdi,%r9
   9ca72:	and    $0x3,%r9d
   9ca76:	divq   0x140(%rcx)
   9ca7d:	shl    $0x6,%edx
   9ca80:	lea    (%rdx,%rax,1),%r8d
   9ca84:	mov    0x130(%rcx),%eax
   9ca8a:	mov    $0xffffffff,%ecx
   9ca8f:	mov    0x88(%rsi),%rdx
   9ca96:	imul   %r9,%rax
   9ca9a:	add    %r8,%rax
   9ca9d:	lock xadd %cx,(%rdx,%rax,2)
   9caa3:	test   %cx,%cx
   9caa6:	jne    9c7f5 <clockcache_get_from_disk_async.constprop.0+0xa5>
   9caac:	push   %rdx
   9caad:	lea    0x14b7a(%rip),%rcx        # b162e <_fini+0xa12>
   9cab4:	mov    $0x288,%esi
   9cab9:	xor    %eax,%eax
   9cabb:	push   %r9
   9cabd:	lea    0x25f4c(%rip),%rdx        # c2a10 <__func__.28.lto_priv.1>
   9cac4:	xor    %r9d,%r9d
   9cac7:	push   %r8
   9cac9:	lea    0x1b728(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9cad0:	push   %rdi
   9cad1:	lea    0x14aff(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9cad8:	call   ee00 <platform_assert_false@plt>
   9cadd:	nopl   (%rax)
   9cae0:	mov    0x384e9(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9cae7:	mov    %rbp,%r8
   9caea:	xor    %eax,%eax
   9caec:	mov    $0x164,%ecx
   9caf1:	lea    0x25dd8(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   9caf8:	lea    0x1a9a9(%rip),%rsi        # b74a8 <__func__.21+0x18>
   9caff:	mov    (%r12),%rdi
   9cb03:	call   fbf0 <fprintf@plt>
   9cb08:	mov    (%r12),%rdi
   9cb0c:	call   fe90 <fflush@plt>
   9cb11:	jmp    9c9d2 <clockcache_get_from_disk_async.constprop.0+0x282>
   9cb16:	cs nopw 0x0(%rax,%rax,1)
   9cb20:	mov    0x80(%rbx),%rax
   9cb27:	mov    %rbp,%rdi
   9cb2a:	call   *0x20(%rax)
   9cb2d:	mov    0x70(%rbx),%ecx
   9cb30:	mov    0x28(%rbx),%rsi
   9cb34:	mov    0x20(%rbx),%rdi
   9cb38:	mov    0x60(%rbx),%edx
   9cb3b:	call   537f0 <clockcache_finish_load.lto_priv.0>
   9cb40:	mov    0x70(%rbx),%eax
   9cb43:	mov    0x20(%rbx),%rsi
   9cb47:	xor    %edx,%edx
   9cb49:	mov    0x58(%rbx),%rdi
   9cb4d:	mov    0x8(%rsi),%rcx
   9cb51:	mov    %eax,0x50(%rbx)
   9cb54:	mov    0x60(%rbx),%eax
   9cb57:	mov    %rdi,%r9
   9cb5a:	and    $0x3,%r9d
   9cb5e:	divq   0x140(%rcx)
   9cb65:	shl    $0x6,%edx
   9cb68:	lea    (%rdx,%rax,1),%r8d
   9cb6c:	mov    0x130(%rcx),%eax
   9cb72:	mov    $0xffffffff,%ecx
   9cb77:	mov    0x88(%rsi),%rdx
   9cb7e:	imul   %r9,%rax
   9cb82:	add    %r8,%rax
   9cb85:	lock xadd %cx,(%rdx,%rax,2)
   9cb8b:	test   %cx,%cx
   9cb8e:	jne    9c7f5 <clockcache_get_from_disk_async.constprop.0+0xa5>
   9cb94:	push   %r10
   9cb96:	lea    0x14a91(%rip),%rcx        # b162e <_fini+0xa12>
   9cb9d:	mov    $0x288,%esi
   9cba2:	xor    %eax,%eax
   9cba4:	push   %r9
   9cba6:	lea    0x25e63(%rip),%rdx        # c2a10 <__func__.28.lto_priv.1>
   9cbad:	xor    %r9d,%r9d
   9cbb0:	push   %r8
   9cbb2:	lea    0x1b63f(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9cbb9:	push   %rdi
   9cbba:	lea    0x14a16(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9cbc1:	call   ee00 <platform_assert_false@plt>
   9cbc6:	cs nopw 0x0(%rax,%rax,1)
   9cbd0:	movl   $0x0,0x54(%rbx)
   9cbd7:	movq   $0x1,0x18(%rbx)
   9cbdf:	jmp    9c804 <clockcache_get_from_disk_async.constprop.0+0xb4>
   9cbe4:	nopl   0x0(%rax)
   9cbe8:	sub    $0x8,%rsp
   9cbec:	mov    %r13,%r8
   9cbef:	mov    %rbp,%r9
   9cbf2:	mov    $0x172,%ecx
   9cbf7:	mov    0x383d2(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9cbfe:	lea    0x25ccb(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   9cc05:	lea    0x1a8ec(%rip),%rsi        # b74f8 <__func__.21+0x68>
   9cc0c:	mov    (%r12),%rdi
   9cc10:	push   $0x0
   9cc12:	call   fbf0 <fprintf@plt>
   9cc17:	mov    (%r12),%rdi
   9cc1b:	call   fe90 <fflush@plt>
   9cc20:	pop    %r13
   9cc22:	pop    %rax
   9cc23:	jmp    9c9d2 <clockcache_get_from_disk_async.constprop.0+0x282>
   9cc28:	nopl   0x0(%rax,%rax,1)
   9cc30:	mov    0x28(%rbx),%rsi
   9cc34:	mov    0x20(%rbx),%rdi
   9cc38:	mov    %eax,%ecx
   9cc3a:	mov    0x60(%rbx),%edx
   9cc3d:	call   537f0 <clockcache_finish_load.lto_priv.0>
   9cc42:	mov    0x70(%rbx),%eax
   9cc45:	mov    0x20(%rbx),%rsi
   9cc49:	xor    %edx,%edx
   9cc4b:	mov    0x58(%rbx),%rdi
   9cc4f:	mov    0x8(%rsi),%rcx
   9cc53:	mov    %eax,0x50(%rbx)
   9cc56:	mov    0x60(%rbx),%eax
   9cc59:	mov    %rdi,%r9
   9cc5c:	and    $0x3,%r9d
   9cc60:	divq   0x140(%rcx)
   9cc67:	shl    $0x6,%edx
   9cc6a:	lea    (%rdx,%rax,1),%r8d
   9cc6e:	mov    0x130(%rcx),%eax
   9cc74:	mov    $0xffffffff,%ecx
   9cc79:	mov    0x88(%rsi),%rdx
   9cc80:	imul   %r9,%rax
   9cc84:	add    %r8,%rax
   9cc87:	lock xadd %cx,(%rdx,%rax,2)
   9cc8d:	test   %cx,%cx
   9cc90:	jne    9c7f5 <clockcache_get_from_disk_async.constprop.0+0xa5>
   9cc96:	push   %r11
   9cc98:	lea    0x1498f(%rip),%rcx        # b162e <_fini+0xa12>
   9cc9f:	mov    $0x288,%esi
   9cca4:	xor    %eax,%eax
   9cca6:	push   %r9
   9cca8:	lea    0x25d61(%rip),%rdx        # c2a10 <__func__.28.lto_priv.1>
   9ccaf:	xor    %r9d,%r9d
   9ccb2:	push   %r8
   9ccb4:	lea    0x1b53d(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9ccbb:	push   %rdi
   9ccbc:	lea    0x14914(%rip),%rdi        # b15d7 <_fini+0x9bb>
   9ccc3:	call   ee00 <platform_assert_false@plt>
   9ccc8:	nopl   0x0(%rax,%rax,1)
   9ccd0:	mov    %rsp,%rsi
   9ccd3:	mov    $0x1,%edi
   9ccd8:	call   e410 <clock_gettime@plt>
   9ccdd:	imul   $0x3b9aca00,(%rsp),%rax
   9cce5:	add    0x8(%rsp),%rax
   9ccea:	mov    %rax,0x480(%rbx)
   9ccf1:	jmp    9c980 <clockcache_get_from_disk_async.constprop.0+0x230>

Disassembly of section .fini:
