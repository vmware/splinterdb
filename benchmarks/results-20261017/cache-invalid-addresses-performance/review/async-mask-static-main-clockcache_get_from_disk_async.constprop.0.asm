
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009c7d0 <clockcache_get_from_disk_async.constprop.0>:
   9c7d0:	mov    0x18(%rdi),%rax
   9c7d4:	cmp    $0x1,%rax
   9c7d8:	je     9ca00 <clockcache_get_from_disk_async.constprop.0+0x230>
   9c7de:	push   %r12
   9c7e0:	push   %rbp
   9c7e1:	push   %rbx
   9c7e2:	mov    %rdi,%rbx
   9c7e5:	sub    $0x10,%rsp
   9c7e9:	test   %rax,%rax
   9c7ec:	je     9c900 <clockcache_get_from_disk_async.constprop.0+0x130>
   9c7f2:	lea    0x80(%rdi),%rbp
   9c7f9:	mov    0x80(%rbx),%rax
   9c800:	mov    %rbp,%rdi
   9c803:	call   *0x8(%rax)
   9c806:	cmp    $0x1,%eax
   9c809:	jne    9c890 <clockcache_get_from_disk_async.constprop.0+0xc0>
   9c80f:	mov    0x80(%rbx),%rax
   9c816:	mov    %rbp,%rdi
   9c819:	call   *0x10(%rax)
   9c81c:	mov    0x20(%rbx),%rdx
   9c820:	mov    %eax,0x70(%rbx)
   9c823:	mov    0x60(%rbx),%eax
   9c826:	shl    $0x6,%rax
   9c82a:	add    0x48(%rdx),%rax
   9c82e:	mov    0x10(%rax),%eax
   9c831:	test   $0x10,%al
   9c833:	jne    9c9e0 <clockcache_get_from_disk_async.constprop.0+0x210>
   9c839:	mov    0x70(%rbx),%edi
   9c83c:	test   %edi,%edi
   9c83e:	jne    9c9a8 <clockcache_get_from_disk_async.constprop.0+0x1d8>
   9c844:	mov    0x80(%rbx),%rax
   9c84b:	mov    %rbp,%rdi
   9c84e:	call   *0x20(%rax)
   9c851:	mov    0x20(%rbx),%rsi
   9c855:	mov    0x70(%rbx),%edi
   9c858:	mov    0x8(%rsi),%rcx
   9c85c:	test   %edi,%edi
   9c85e:	jne    9ca10 <clockcache_get_from_disk_async.constprop.0+0x240>
   9c864:	mov    0x18(%rcx),%eax
   9c867:	test   %eax,%eax
   9c869:	jne    9c8a0 <clockcache_get_from_disk_async.constprop.0+0xd0>
   9c86b:	mov    0x68(%rbx),%rax
   9c86f:	mov    %rax,0x48(%rbx)
   9c873:	movl   $0x1,0x54(%rbx)
   9c87a:	movq   $0x1,0x18(%rbx)
   9c882:	add    $0x10,%rsp
   9c886:	mov    $0x1,%eax
   9c88b:	pop    %rbx
   9c88c:	pop    %rbp
   9c88d:	pop    %r12
   9c88f:	ret
   9c890:	add    $0x10,%rsp
   9c894:	xor    %eax,%eax
   9c896:	pop    %rbx
   9c897:	pop    %rbp
   9c898:	pop    %r12
   9c89a:	ret
   9c89b:	nopl   0x0(%rax,%rax,1)
   9c8a0:	mov    %rsp,%rsi
   9c8a3:	mov    $0x1,%edi
   9c8a8:	mov    0x480(%rbx),%rbp
   9c8af:	call   e410 <clock_gettime@plt>
   9c8b4:	mov    0x58(%rbx),%rcx
   9c8b8:	imul   $0x3b9aca00,(%rsp),%rdx
   9c8c0:	add    0x8(%rsp),%rdx
   9c8c5:	lea    0x0(,%rcx,8),%rax
   9c8cd:	sub    %rbp,%rdx
   9c8d0:	sub    %rcx,%rax
   9c8d3:	mov    0x30(%rbx),%ecx
   9c8d6:	lea    (%rcx,%rax,8),%rcx
   9c8da:	mov    0x20(%rbx),%rax
   9c8de:	lea    (%rax,%rcx,8),%rax
   9c8e2:	addq   $0x1,0x1148(%rax)
   9c8ea:	addq   $0x1,0x1220(%rax)
   9c8f2:	add    %rdx,0x1190(%rax)
   9c8f9:	jmp    9c86b <clockcache_get_from_disk_async.constprop.0+0x9b>
   9c8fe:	xchg   %ax,%ax
   9c900:	mov    0x30(%rdi),%edx
   9c903:	mov    0x28(%rdi),%rsi
   9c907:	mov    0x20(%rdi),%rdi
   9c90b:	call   53d00 <clockcache_acquire_entry_for_load.lto_priv.0>
   9c910:	mov    $0xffffffff,%edx
   9c915:	mov    %rax,0x60(%rbx)
   9c919:	cmp    %rdx,%rax
   9c91c:	je     9cb70 <clockcache_get_from_disk_async.constprop.0+0x3a0>
   9c922:	mov    0x20(%rbx),%rdx
   9c926:	shl    $0x6,%rax
   9c92a:	mov    0x28(%rbx),%rcx
   9c92e:	mov    %rbx,%r9
   9c931:	lea    0x80(%rbx),%rbp
   9c938:	mov    0x385f1(%rip),%r8        # d4f30 <clockcache_get_from_disk_async_callback@@Base+0x82630>
   9c93f:	mov    0x18(%rdx),%rsi
   9c943:	add    0x48(%rdx),%rax
   9c947:	mov    %rbp,%rdi
   9c94a:	xor    %edx,%edx
   9c94c:	mov    %rax,0x68(%rbx)
   9c950:	mov    (%rsi),%rax
   9c953:	call   *0x18(%rax)
   9c956:	mov    %eax,0x70(%rbx)
   9c959:	test   %eax,%eax
   9c95b:	jne    9cb88 <clockcache_get_from_disk_async.constprop.0+0x3b8>
   9c961:	mov    0x68(%rbx),%rax
   9c965:	mov    %rbp,%rdi
   9c968:	mov    (%rax),%rsi
   9c96b:	mov    0x80(%rbx),%rax
   9c972:	call   *(%rax)
   9c974:	mov    %eax,0x70(%rbx)
   9c977:	test   %eax,%eax
   9c979:	jne    9cac0 <clockcache_get_from_disk_async.constprop.0+0x2f0>
   9c97f:	mov    0x20(%rbx),%rax
   9c983:	mov    0x8(%rax),%rax
   9c987:	mov    0x18(%rax),%r8d
   9c98b:	test   %r8d,%r8d
   9c98e:	jne    9ca90 <clockcache_get_from_disk_async.constprop.0+0x2c0>
   9c994:	lea    -0x1a2(%rip),%rax        # 9c7f9 <clockcache_get_from_disk_async.constprop.0+0x29>
   9c99b:	mov    %rax,0x18(%rbx)
   9c99f:	jmp    9c7f9 <clockcache_get_from_disk_async.constprop.0+0x29>
   9c9a4:	nopl   0x0(%rax)
   9c9a8:	call   efe0 <strerror@plt>
   9c9ad:	mov    0x3861c(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9c9b4:	mov    0x60(%rbx),%rcx
   9c9b8:	lea    0x240f1(%rip),%rsi        # c0ab0 <__func__.21+0x95f0>
   9c9bf:	mov    0x28(%rbx),%rdx
   9c9c3:	mov    %rax,%r8
   9c9c6:	xor    %eax,%eax
   9c9c8:	mov    (%r12),%rdi
   9c9cc:	call   fbf0 <fprintf@plt>
   9c9d1:	mov    (%r12),%rdi
   9c9d5:	call   fe90 <fflush@plt>
   9c9da:	jmp    9c844 <clockcache_get_from_disk_async.constprop.0+0x74>
   9c9df:	nop
   9c9e0:	mov    0x70(%rbx),%ecx
   9c9e3:	mov    0x28(%rbx),%rsi
   9c9e7:	mov    0x20(%rbx),%rdi
   9c9eb:	mov    0x60(%rbx),%edx
   9c9ee:	call   52660 <clockcache_finish_load.lto_priv.0>
   9c9f3:	jmp    9c839 <clockcache_get_from_disk_async.constprop.0+0x69>
   9c9f8:	nopl   0x0(%rax,%rax,1)
   9ca00:	mov    $0x1,%eax
   9ca05:	ret
   9ca06:	cs nopw 0x0(%rax,%rax,1)
   9ca10:	mov    0x70(%rbx),%eax
   9ca13:	xor    %edx,%edx
   9ca15:	mov    0x58(%rbx),%rdi
   9ca19:	mov    %eax,0x50(%rbx)
   9ca1c:	mov    0x60(%rbx),%eax
   9ca1f:	mov    %rdi,%r9
   9ca22:	and    $0x3,%r9d
   9ca26:	divq   0x140(%rcx)
   9ca2d:	shl    $0x6,%edx
   9ca30:	lea    (%rdx,%rax,1),%r8d
   9ca34:	mov    0x130(%rcx),%eax
   9ca3a:	mov    $0xffffffff,%ecx
   9ca3f:	mov    0x88(%rsi),%rdx
   9ca46:	imul   %r9,%rax
   9ca4a:	add    %r8,%rax
   9ca4d:	lock xadd %cx,(%rdx,%rax,2)
   9ca53:	test   %cx,%cx
   9ca56:	jne    9c873 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9ca5c:	push   %rdx
   9ca5d:	lea    0x14bc0(%rip),%rcx        # b1624 <_fini+0xa48>
   9ca64:	mov    $0x261,%esi
   9ca69:	xor    %eax,%eax
   9ca6b:	push   %r9
   9ca6d:	lea    0x25f1c(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   9ca74:	xor    %r9d,%r9d
   9ca77:	push   %r8
   9ca79:	lea    0x1b770(%rip),%r8        # b81f0 <__func__.21+0xd30>
   9ca80:	push   %rdi
   9ca81:	lea    0x14b4f(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9ca88:	call   ee00 <platform_assert_false@plt>
   9ca8d:	nopl   (%rax)
   9ca90:	mov    %rsp,%rsi
   9ca93:	mov    $0x1,%edi
   9ca98:	call   e410 <clock_gettime@plt>
   9ca9d:	imul   $0x3b9aca00,(%rsp),%rax
   9caa5:	add    0x8(%rsp),%rax
   9caaa:	mov    %rax,0x480(%rbx)
   9cab1:	jmp    9c994 <clockcache_get_from_disk_async.constprop.0+0x1c4>
   9cab6:	cs nopw 0x0(%rax,%rax,1)
   9cac0:	mov    0x80(%rbx),%rax
   9cac7:	mov    %rbp,%rdi
   9caca:	call   *0x20(%rax)
   9cacd:	mov    0x70(%rbx),%ecx
   9cad0:	mov    0x28(%rbx),%rsi
   9cad4:	mov    0x20(%rbx),%rdi
   9cad8:	mov    0x60(%rbx),%edx
   9cadb:	call   52660 <clockcache_finish_load.lto_priv.0>
   9cae0:	mov    0x70(%rbx),%eax
   9cae3:	mov    0x20(%rbx),%rsi
   9cae7:	xor    %edx,%edx
   9cae9:	mov    0x58(%rbx),%rdi
   9caed:	mov    0x8(%rsi),%rcx
   9caf1:	mov    %eax,0x50(%rbx)
   9caf4:	mov    0x60(%rbx),%eax
   9caf7:	mov    %rdi,%r9
   9cafa:	and    $0x3,%r9d
   9cafe:	divq   0x140(%rcx)
   9cb05:	shl    $0x6,%edx
   9cb08:	lea    (%rdx,%rax,1),%r8d
   9cb0c:	mov    0x130(%rcx),%eax
   9cb12:	mov    $0xffffffff,%ecx
   9cb17:	mov    0x88(%rsi),%rdx
   9cb1e:	imul   %r9,%rax
   9cb22:	add    %r8,%rax
   9cb25:	lock xadd %cx,(%rdx,%rax,2)
   9cb2b:	test   %cx,%cx
   9cb2e:	jne    9c873 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9cb34:	push   %r10
   9cb36:	lea    0x14ae7(%rip),%rcx        # b1624 <_fini+0xa48>
   9cb3d:	mov    $0x261,%esi
   9cb42:	xor    %eax,%eax
   9cb44:	push   %r9
   9cb46:	lea    0x25e43(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   9cb4d:	xor    %r9d,%r9d
   9cb50:	push   %r8
   9cb52:	lea    0x1b697(%rip),%r8        # b81f0 <__func__.21+0xd30>
   9cb59:	push   %rdi
   9cb5a:	lea    0x14a76(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9cb61:	call   ee00 <platform_assert_false@plt>
   9cb66:	cs nopw 0x0(%rax,%rax,1)
   9cb70:	movl   $0x0,0x54(%rbx)
   9cb77:	movq   $0x1,0x18(%rbx)
   9cb7f:	jmp    9c882 <clockcache_get_from_disk_async.constprop.0+0xb2>
   9cb84:	nopl   0x0(%rax)
   9cb88:	mov    0x28(%rbx),%rsi
   9cb8c:	mov    0x20(%rbx),%rdi
   9cb90:	mov    %eax,%ecx
   9cb92:	mov    0x60(%rbx),%edx
   9cb95:	call   52660 <clockcache_finish_load.lto_priv.0>
   9cb9a:	mov    0x70(%rbx),%eax
   9cb9d:	mov    0x20(%rbx),%rsi
   9cba1:	xor    %edx,%edx
   9cba3:	mov    0x58(%rbx),%rdi
   9cba7:	mov    0x8(%rsi),%rcx
   9cbab:	mov    %eax,0x50(%rbx)
   9cbae:	mov    0x60(%rbx),%eax
   9cbb1:	mov    %rdi,%r9
   9cbb4:	and    $0x3,%r9d
   9cbb8:	divq   0x140(%rcx)
   9cbbf:	shl    $0x6,%edx
   9cbc2:	lea    (%rdx,%rax,1),%r8d
   9cbc6:	mov    0x130(%rcx),%eax
   9cbcc:	mov    $0xffffffff,%ecx
   9cbd1:	mov    0x88(%rsi),%rdx
   9cbd8:	imul   %r9,%rax
   9cbdc:	add    %r8,%rax
   9cbdf:	lock xadd %cx,(%rdx,%rax,2)
   9cbe5:	test   %cx,%cx
   9cbe8:	jne    9c873 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9cbee:	push   %r11
   9cbf0:	lea    0x14a2d(%rip),%rcx        # b1624 <_fini+0xa48>
   9cbf7:	mov    $0x261,%esi
   9cbfc:	xor    %eax,%eax
   9cbfe:	push   %r9
   9cc00:	lea    0x25d89(%rip),%rdx        # c2990 <__func__.28.lto_priv.1>
   9cc07:	xor    %r9d,%r9d
   9cc0a:	push   %r8
   9cc0c:	lea    0x1b5dd(%rip),%r8        # b81f0 <__func__.21+0xd30>
   9cc13:	push   %rdi
   9cc14:	lea    0x149bc(%rip),%rdi        # b15d7 <_fini+0x9fb>
   9cc1b:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
