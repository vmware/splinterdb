
.cache/invalid-address-perf-20261017/builds/candidate/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

000000000009c790 <clockcache_get_from_disk_async.constprop.0>:
   9c790:	mov    0x18(%rdi),%rax
   9c794:	cmp    $0x1,%rax
   9c798:	je     9c9c0 <clockcache_get_from_disk_async.constprop.0+0x230>
   9c79e:	push   %r12
   9c7a0:	push   %rbp
   9c7a1:	push   %rbx
   9c7a2:	mov    %rdi,%rbx
   9c7a5:	sub    $0x10,%rsp
   9c7a9:	test   %rax,%rax
   9c7ac:	je     9c8c0 <clockcache_get_from_disk_async.constprop.0+0x130>
   9c7b2:	lea    0x80(%rdi),%rbp
   9c7b9:	mov    0x80(%rbx),%rax
   9c7c0:	mov    %rbp,%rdi
   9c7c3:	call   *0x8(%rax)
   9c7c6:	cmp    $0x1,%eax
   9c7c9:	jne    9c850 <clockcache_get_from_disk_async.constprop.0+0xc0>
   9c7cf:	mov    0x80(%rbx),%rax
   9c7d6:	mov    %rbp,%rdi
   9c7d9:	call   *0x10(%rax)
   9c7dc:	mov    0x20(%rbx),%rdx
   9c7e0:	mov    %eax,0x70(%rbx)
   9c7e3:	mov    0x60(%rbx),%eax
   9c7e6:	shl    $0x6,%rax
   9c7ea:	add    0x48(%rdx),%rax
   9c7ee:	mov    0x10(%rax),%eax
   9c7f1:	test   $0x10,%al
   9c7f3:	jne    9c9a0 <clockcache_get_from_disk_async.constprop.0+0x210>
   9c7f9:	mov    0x70(%rbx),%edi
   9c7fc:	test   %edi,%edi
   9c7fe:	jne    9c968 <clockcache_get_from_disk_async.constprop.0+0x1d8>
   9c804:	mov    0x80(%rbx),%rax
   9c80b:	mov    %rbp,%rdi
   9c80e:	call   *0x20(%rax)
   9c811:	mov    0x20(%rbx),%rsi
   9c815:	mov    0x70(%rbx),%edi
   9c818:	mov    0x8(%rsi),%rcx
   9c81c:	test   %edi,%edi
   9c81e:	jne    9c9d0 <clockcache_get_from_disk_async.constprop.0+0x240>
   9c824:	mov    0x18(%rcx),%eax
   9c827:	test   %eax,%eax
   9c829:	jne    9c860 <clockcache_get_from_disk_async.constprop.0+0xd0>
   9c82b:	mov    0x68(%rbx),%rax
   9c82f:	mov    %rax,0x48(%rbx)
   9c833:	movl   $0x1,0x54(%rbx)
   9c83a:	movq   $0x1,0x18(%rbx)
   9c842:	add    $0x10,%rsp
   9c846:	mov    $0x1,%eax
   9c84b:	pop    %rbx
   9c84c:	pop    %rbp
   9c84d:	pop    %r12
   9c84f:	ret
   9c850:	add    $0x10,%rsp
   9c854:	xor    %eax,%eax
   9c856:	pop    %rbx
   9c857:	pop    %rbp
   9c858:	pop    %r12
   9c85a:	ret
   9c85b:	nopl   0x0(%rax,%rax,1)
   9c860:	mov    %rsp,%rsi
   9c863:	mov    $0x1,%edi
   9c868:	mov    0x480(%rbx),%rbp
   9c86f:	call   e410 <clock_gettime@plt>
   9c874:	mov    0x58(%rbx),%rcx
   9c878:	imul   $0x3b9aca00,(%rsp),%rdx
   9c880:	add    0x8(%rsp),%rdx
   9c885:	lea    0x0(,%rcx,8),%rax
   9c88d:	sub    %rbp,%rdx
   9c890:	sub    %rcx,%rax
   9c893:	mov    0x30(%rbx),%ecx
   9c896:	lea    (%rcx,%rax,8),%rcx
   9c89a:	mov    0x20(%rbx),%rax
   9c89e:	lea    (%rax,%rcx,8),%rax
   9c8a2:	addq   $0x1,0x1148(%rax)
   9c8aa:	addq   $0x1,0x1220(%rax)
   9c8b2:	add    %rdx,0x1190(%rax)
   9c8b9:	jmp    9c82b <clockcache_get_from_disk_async.constprop.0+0x9b>
   9c8be:	xchg   %ax,%ax
   9c8c0:	mov    0x30(%rdi),%edx
   9c8c3:	mov    0x28(%rdi),%rsi
   9c8c7:	mov    0x20(%rdi),%rdi
   9c8cb:	call   53800 <clockcache_acquire_entry_for_load.lto_priv.0>
   9c8d0:	mov    $0xffffffff,%edx
   9c8d5:	mov    %rax,0x60(%rbx)
   9c8d9:	cmp    %rdx,%rax
   9c8dc:	je     9cb30 <clockcache_get_from_disk_async.constprop.0+0x3a0>
   9c8e2:	mov    0x20(%rbx),%rdx
   9c8e6:	shl    $0x6,%rax
   9c8ea:	mov    0x28(%rbx),%rcx
   9c8ee:	mov    %rbx,%r9
   9c8f1:	lea    0x80(%rbx),%rbp
   9c8f8:	mov    0x38631(%rip),%r8        # d4f30 <clockcache_get_from_disk_async_callback@@Base+0x80e30>
   9c8ff:	mov    0x18(%rdx),%rsi
   9c903:	add    0x48(%rdx),%rax
   9c907:	mov    %rbp,%rdi
   9c90a:	xor    %edx,%edx
   9c90c:	mov    %rax,0x68(%rbx)
   9c910:	mov    (%rsi),%rax
   9c913:	call   *0x18(%rax)
   9c916:	mov    %eax,0x70(%rbx)
   9c919:	test   %eax,%eax
   9c91b:	jne    9cb48 <clockcache_get_from_disk_async.constprop.0+0x3b8>
   9c921:	mov    0x68(%rbx),%rax
   9c925:	mov    %rbp,%rdi
   9c928:	mov    (%rax),%rsi
   9c92b:	mov    0x80(%rbx),%rax
   9c932:	call   *(%rax)
   9c934:	mov    %eax,0x70(%rbx)
   9c937:	test   %eax,%eax
   9c939:	jne    9ca80 <clockcache_get_from_disk_async.constprop.0+0x2f0>
   9c93f:	mov    0x20(%rbx),%rax
   9c943:	mov    0x8(%rax),%rax
   9c947:	mov    0x18(%rax),%r8d
   9c94b:	test   %r8d,%r8d
   9c94e:	jne    9ca50 <clockcache_get_from_disk_async.constprop.0+0x2c0>
   9c954:	lea    -0x1a2(%rip),%rax        # 9c7b9 <clockcache_get_from_disk_async.constprop.0+0x29>
   9c95b:	mov    %rax,0x18(%rbx)
   9c95f:	jmp    9c7b9 <clockcache_get_from_disk_async.constprop.0+0x29>
   9c964:	nopl   0x0(%rax)
   9c968:	call   efe0 <strerror@plt>
   9c96d:	mov    0x3865c(%rip),%r12        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   9c974:	mov    0x60(%rbx),%rcx
   9c978:	lea    0x24169(%rip),%rsi        # c0ae8 <__func__.21+0x9658>
   9c97f:	mov    0x28(%rbx),%rdx
   9c983:	mov    %rax,%r8
   9c986:	xor    %eax,%eax
   9c988:	mov    (%r12),%rdi
   9c98c:	call   fbf0 <fprintf@plt>
   9c991:	mov    (%r12),%rdi
   9c995:	call   fe90 <fflush@plt>
   9c99a:	jmp    9c804 <clockcache_get_from_disk_async.constprop.0+0x74>
   9c99f:	nop
   9c9a0:	mov    0x70(%rbx),%ecx
   9c9a3:	mov    0x28(%rbx),%rsi
   9c9a7:	mov    0x20(%rbx),%rdi
   9c9ab:	mov    0x60(%rbx),%edx
   9c9ae:	call   53930 <clockcache_finish_load.lto_priv.0>
   9c9b3:	jmp    9c7f9 <clockcache_get_from_disk_async.constprop.0+0x69>
   9c9b8:	nopl   0x0(%rax,%rax,1)
   9c9c0:	mov    $0x1,%eax
   9c9c5:	ret
   9c9c6:	cs nopw 0x0(%rax,%rax,1)
   9c9d0:	mov    0x70(%rbx),%eax
   9c9d3:	xor    %edx,%edx
   9c9d5:	mov    0x58(%rbx),%rdi
   9c9d9:	mov    %eax,0x50(%rbx)
   9c9dc:	mov    0x60(%rbx),%eax
   9c9df:	mov    %rdi,%r9
   9c9e2:	and    $0x3,%r9d
   9c9e6:	divq   0x140(%rcx)
   9c9ed:	shl    $0x6,%edx
   9c9f0:	lea    (%rdx,%rax,1),%r8d
   9c9f4:	mov    0x130(%rcx),%eax
   9c9fa:	mov    $0xffffffff,%ecx
   9c9ff:	mov    0x88(%rsi),%rdx
   9ca06:	imul   %r9,%rax
   9ca0a:	add    %r8,%rax
   9ca0d:	lock xadd %cx,(%rdx,%rax,2)
   9ca13:	test   %cx,%cx
   9ca16:	jne    9c833 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9ca1c:	push   %rdx
   9ca1d:	lea    0x14c0a(%rip),%rcx        # b162e <_fini+0xb12>
   9ca24:	mov    $0x290,%esi
   9ca29:	xor    %eax,%eax
   9ca2b:	push   %r9
   9ca2d:	lea    0x25f9c(%rip),%rdx        # c29d0 <__func__.28.lto_priv.1>
   9ca34:	xor    %r9d,%r9d
   9ca37:	push   %r8
   9ca39:	lea    0x1b7b8(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9ca40:	push   %rdi
   9ca41:	lea    0x14b8f(%rip),%rdi        # b15d7 <_fini+0xabb>
   9ca48:	call   ee00 <platform_assert_false@plt>
   9ca4d:	nopl   (%rax)
   9ca50:	mov    %rsp,%rsi
   9ca53:	mov    $0x1,%edi
   9ca58:	call   e410 <clock_gettime@plt>
   9ca5d:	imul   $0x3b9aca00,(%rsp),%rax
   9ca65:	add    0x8(%rsp),%rax
   9ca6a:	mov    %rax,0x480(%rbx)
   9ca71:	jmp    9c954 <clockcache_get_from_disk_async.constprop.0+0x1c4>
   9ca76:	cs nopw 0x0(%rax,%rax,1)
   9ca80:	mov    0x80(%rbx),%rax
   9ca87:	mov    %rbp,%rdi
   9ca8a:	call   *0x20(%rax)
   9ca8d:	mov    0x70(%rbx),%ecx
   9ca90:	mov    0x28(%rbx),%rsi
   9ca94:	mov    0x20(%rbx),%rdi
   9ca98:	mov    0x60(%rbx),%edx
   9ca9b:	call   53930 <clockcache_finish_load.lto_priv.0>
   9caa0:	mov    0x70(%rbx),%eax
   9caa3:	mov    0x20(%rbx),%rsi
   9caa7:	xor    %edx,%edx
   9caa9:	mov    0x58(%rbx),%rdi
   9caad:	mov    0x8(%rsi),%rcx
   9cab1:	mov    %eax,0x50(%rbx)
   9cab4:	mov    0x60(%rbx),%eax
   9cab7:	mov    %rdi,%r9
   9caba:	and    $0x3,%r9d
   9cabe:	divq   0x140(%rcx)
   9cac5:	shl    $0x6,%edx
   9cac8:	lea    (%rdx,%rax,1),%r8d
   9cacc:	mov    0x130(%rcx),%eax
   9cad2:	mov    $0xffffffff,%ecx
   9cad7:	mov    0x88(%rsi),%rdx
   9cade:	imul   %r9,%rax
   9cae2:	add    %r8,%rax
   9cae5:	lock xadd %cx,(%rdx,%rax,2)
   9caeb:	test   %cx,%cx
   9caee:	jne    9c833 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9caf4:	push   %r10
   9caf6:	lea    0x14b31(%rip),%rcx        # b162e <_fini+0xb12>
   9cafd:	mov    $0x290,%esi
   9cb02:	xor    %eax,%eax
   9cb04:	push   %r9
   9cb06:	lea    0x25ec3(%rip),%rdx        # c29d0 <__func__.28.lto_priv.1>
   9cb0d:	xor    %r9d,%r9d
   9cb10:	push   %r8
   9cb12:	lea    0x1b6df(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9cb19:	push   %rdi
   9cb1a:	lea    0x14ab6(%rip),%rdi        # b15d7 <_fini+0xabb>
   9cb21:	call   ee00 <platform_assert_false@plt>
   9cb26:	cs nopw 0x0(%rax,%rax,1)
   9cb30:	movl   $0x0,0x54(%rbx)
   9cb37:	movq   $0x1,0x18(%rbx)
   9cb3f:	jmp    9c842 <clockcache_get_from_disk_async.constprop.0+0xb2>
   9cb44:	nopl   0x0(%rax)
   9cb48:	mov    0x28(%rbx),%rsi
   9cb4c:	mov    0x20(%rbx),%rdi
   9cb50:	mov    %eax,%ecx
   9cb52:	mov    0x60(%rbx),%edx
   9cb55:	call   53930 <clockcache_finish_load.lto_priv.0>
   9cb5a:	mov    0x70(%rbx),%eax
   9cb5d:	mov    0x20(%rbx),%rsi
   9cb61:	xor    %edx,%edx
   9cb63:	mov    0x58(%rbx),%rdi
   9cb67:	mov    0x8(%rsi),%rcx
   9cb6b:	mov    %eax,0x50(%rbx)
   9cb6e:	mov    0x60(%rbx),%eax
   9cb71:	mov    %rdi,%r9
   9cb74:	and    $0x3,%r9d
   9cb78:	divq   0x140(%rcx)
   9cb7f:	shl    $0x6,%edx
   9cb82:	lea    (%rdx,%rax,1),%r8d
   9cb86:	mov    0x130(%rcx),%eax
   9cb8c:	mov    $0xffffffff,%ecx
   9cb91:	mov    0x88(%rsi),%rdx
   9cb98:	imul   %r9,%rax
   9cb9c:	add    %r8,%rax
   9cb9f:	lock xadd %cx,(%rdx,%rax,2)
   9cba5:	test   %cx,%cx
   9cba8:	jne    9c833 <clockcache_get_from_disk_async.constprop.0+0xa3>
   9cbae:	push   %r11
   9cbb0:	lea    0x14a77(%rip),%rcx        # b162e <_fini+0xb12>
   9cbb7:	mov    $0x290,%esi
   9cbbc:	xor    %eax,%eax
   9cbbe:	push   %r9
   9cbc0:	lea    0x25e09(%rip),%rdx        # c29d0 <__func__.28.lto_priv.1>
   9cbc7:	xor    %r9d,%r9d
   9cbca:	push   %r8
   9cbcc:	lea    0x1b625(%rip),%r8        # b81f8 <__func__.21+0xd68>
   9cbd3:	push   %rdi
   9cbd4:	lea    0x149fc(%rip),%rdi        # b15d7 <_fini+0xabb>
   9cbdb:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
