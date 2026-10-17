
.cache/invalid-address-perf-20261017/builds/async-load/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053a20 <clockcache_get>:
   53a20:	push   %r15
   53a22:	lea    -0x1(%rcx),%eax
   53a25:	push   %r14
   53a27:	push   %r13
   53a29:	push   %r12
   53a2b:	push   %rbp
   53a2c:	push   %rbx
   53a2d:	mov    %rsi,%rbx
   53a30:	sub    $0x48,%rsp
   53a34:	mov    %edx,0x8(%rsp)
   53a38:	mov    %ecx,0x1c(%rsp)
   53a3c:	mov    %r8,0x20(%rsp)
   53a41:	cmp    $0x7,%eax
   53a44:	ja     54038 <clockcache_get+0x618>
   53a4a:	mov    0x8(%rdi),%rcx
   53a4e:	mov    %rdi,%r14
   53a51:	mov    %rsi,%rax
   53a54:	xor    %edx,%edx
   53a56:	mov    0x8(%rcx),%rdi
   53a5a:	divq   0x8(%rdi)
   53a5e:	test   %rdx,%rdx
   53a61:	jne    1015d <clockcache_get.cold>
   53a67:	movabs $0xcccccccccccccccd,%r12
   53a71:	mov    %r14,%r13
   53a74:	data16 cs nopw 0x0(%rax,%rax,1)
   53a7f:	nop
   53a80:	mov    0x120(%rcx),%rcx
   53a87:	mov    0x28(%r13),%rax
   53a8b:	mov    %rbx,%rdx
   53a8e:	shr    %cl,%rdx
   53a91:	shr    $0x2,%rax
   53a95:	cmp    %rax,%rdx
   53a98:	jae    5409b <clockcache_get+0x67b>
   53a9e:	mov    0x30(%r13),%rax
   53aa2:	mov    (%rax,%rdx,4),%ebp
   53aa5:	cmp    $0xffffffff,%ebp
   53aa8:	je     53c50 <clockcache_get+0x230>
   53aae:	data16 lea 0x814f2(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53ab6:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53abe:	mov    0x8(%rsp),%r14d
   53ac3:	mov    %ebp,%esi
   53ac5:	mov    %r13,%rdi
   53ac8:	mov    (%rax),%rax
   53acb:	mov    %rax,0x10(%rsp)
   53ad0:	test   %r14d,%r14d
   53ad3:	je     53c10 <clockcache_get+0x1f0>
   53ad9:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53ade:	cmp    $0x1,%eax
   53ae1:	jne    53b2d <clockcache_get+0x10d>
   53ae3:	mov    $0x1,%r15d
   53ae9:	mov    %r15,%rax
   53aec:	mul    %r12
   53aef:	xor    %eax,%eax
   53af1:	shr    $0x2,%rdx
   53af5:	mov    %rdx,%r14
   53af8:	nopl   0x0(%rax,%rax,1)
   53b00:	pause
   53b02:	mov    %rax,%rcx
   53b05:	add    $0x1,%rax
   53b09:	cmp    %r14,%rcx
   53b0c:	jne    53b00 <clockcache_get+0xe0>
   53b0e:	cmp    $0x400,%r15
   53b15:	ja     53be0 <clockcache_get+0x1c0>
   53b1b:	mov    %ebp,%esi
   53b1d:	mov    %r13,%rdi
   53b20:	add    %r15,%r15
   53b23:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53b28:	cmp    $0x1,%eax
   53b2b:	je     53ae9 <clockcache_get+0xc9>
   53b2d:	test   %eax,%eax
   53b2f:	jne    53c27 <clockcache_get+0x207>
   53b35:	mov    %ebp,%eax
   53b37:	mov    0x48(%r13),%rcx
   53b3b:	mov    %rax,%rdx
   53b3e:	shl    $0x6,%rdx
   53b42:	mov    0x8(%rcx,%rdx,1),%rsi
   53b47:	cmp    %rsi,%rbx
   53b4a:	jne    53e8c <clockcache_get+0x46c>
   53b50:	mov    %rax,%rbx
   53b53:	mov    %rdx,%r15
   53b56:	mov    %r13,%r14
   53b59:	jmp    53b6c <clockcache_get+0x14c>
   53b5b:	nopl   0x0(%rax,%rax,1)
   53b60:	mov    %r14,%rdi
   53b63:	call   ea40 <clockcache_wait@plt>
   53b68:	mov    0x48(%r14),%rcx
   53b6c:	mov    0x10(%rcx,%r15,1),%eax
   53b71:	test   $0x10,%al
   53b73:	jne    53b60 <clockcache_get+0x140>
   53b75:	mov    0x48(%r14),%rdx
   53b79:	mov    %rbx,%rax
   53b7c:	add    %r15,%rdx
   53b7f:	mov    0x14(%rdx),%r13d
   53b83:	test   %r13d,%r13d
   53b86:	jne    53fea <clockcache_get+0x5ca>
   53b8c:	mov    0x20(%rdx),%eax
   53b8f:	cmp    %eax,0x1c(%rsp)
   53b93:	jne    5406a <clockcache_get+0x64a>
   53b99:	mov    0x8(%r14),%rax
   53b9d:	mov    0x18(%rax),%r11d
   53ba1:	test   %r11d,%r11d
   53ba4:	jne    53e66 <clockcache_get+0x446>
   53baa:	mov    0x20(%rsp),%rbx
   53baf:	xor    %r13d,%r13d
   53bb2:	mov    %rdx,(%rbx)
   53bb5:	data16 lea 0x813eb(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53bbd:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53bc5:	mov    (%rax),%rax
   53bc8:	mov    %rax,0x8(%rbx)
   53bcc:	add    $0x48,%rsp
   53bd0:	mov    %r13d,%eax
   53bd3:	pop    %rbx
   53bd4:	pop    %rbp
   53bd5:	pop    %r12
   53bd7:	pop    %r13
   53bd9:	pop    %r14
   53bdb:	pop    %r15
   53bdd:	ret
   53bde:	xchg   %ax,%ax
   53be0:	mov    %ebp,%esi
   53be2:	mov    %r13,%rdi
   53be5:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53bea:	cmp    $0x1,%eax
   53bed:	jne    53b2d <clockcache_get+0x10d>
   53bf3:	xor    %eax,%eax
   53bf5:	data16 cs nopw 0x0(%rax,%rax,1)
   53c00:	pause
   53c02:	mov    %rax,%rdx
   53c05:	add    $0x1,%rax
   53c09:	cmp    %r14,%rdx
   53c0c:	jne    53c00 <clockcache_get+0x1e0>
   53c0e:	jmp    53be0 <clockcache_get+0x1c0>
   53c10:	call   aa3e0 <clockcache_try_get_read.constprop.0>
   53c15:	cmp    $0x1,%eax
   53c18:	je     53ca6 <clockcache_get+0x286>
   53c1e:	cmp    $0x2,%eax
   53c21:	jne    53b35 <clockcache_get+0x115>
   53c27:	mov    0x8(%r13),%rcx
   53c2b:	mov    %rbx,%rax
   53c2e:	xor    %edx,%edx
   53c30:	mov    0x8(%rcx),%rsi
   53c34:	divq   0x8(%rsi)
   53c38:	test   %rdx,%rdx
   53c3b:	je     53a80 <clockcache_get+0x60>
   53c41:	mov    %r13,%r14
   53c44:	jmp    1015d <clockcache_get.cold>
   53c49:	nopl   0x0(%rax)
   53c50:	mov    0x10(%r13),%rbp
   53c54:	mov    0x0(%rbp),%rax
   53c58:	mov    %rbp,%rdi
   53c5b:	call   *(%rax)
   53c5d:	xor    %edx,%edx
   53c5f:	mov    %rax,%rcx
   53c62:	mov    (%rax),%rax
   53c65:	mov    0x8(%rax),%r9
   53c69:	mov    %rbx,%rax
   53c6c:	div    %r9
   53c6f:	test   %rdx,%rdx
   53c72:	jne    53e30 <clockcache_get+0x410>
   53c78:	mov    0x0(%rbp),%rax
   53c7c:	mov    %rbx,%r15
   53c7f:	and    0x20(%rcx),%r15
   53c83:	je     53cb8 <clockcache_get+0x298>
   53c85:	cmp    0x8(%rcx),%rbx
   53c89:	jae    53cb8 <clockcache_get+0x298>
   53c8b:	mov    %r15,%rsi
   53c8e:	mov    %rbp,%rdi
   53c91:	call   *0x20(%rax)
   53c94:	test   %eax,%eax
   53c96:	je     53ee0 <clockcache_get+0x4c0>
   53c9c:	mov    0x8(%rsp),%r8d
   53ca1:	test   %r8d,%r8d
   53ca4:	jne    53d04 <clockcache_get+0x2e4>
   53ca6:	mov    $0xb,%r13d
   53cac:	jmp    53bcc <clockcache_get+0x1ac>
   53cb1:	nopl   0x0(%rax)
   53cb8:	mov    %rbp,%rdi
   53cbb:	call   *0x58(%rax)
   53cbe:	mov    0x8130b(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53cc5:	mov    %rbx,%r9
   53cc8:	mov    %r15,%r8
   53ccb:	sub    $0x8,%rsp
   53ccf:	lea    0x6388a(%rip),%rsi        # b7560 <__func__.21+0xd0>
   53cd6:	mov    $0x17d,%ecx
   53cdb:	mov    0x0(%rbp),%rdi
   53cdf:	push   %rax
   53ce0:	lea    0x6ebe9(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   53ce7:	xor    %eax,%eax
   53ce9:	call   fbf0 <fprintf@plt>
   53cee:	mov    0x0(%rbp),%rdi
   53cf2:	call   fe90 <fflush@plt>
   53cf7:	pop    %rsi
   53cf8:	pop    %rdi
   53cf9:	mov    $0x16,%r13d
   53cff:	jmp    53bcc <clockcache_get+0x1ac>
   53d04:	data16 lea 0x8129c(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53d0c:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53d14:	mov    0x1c(%rsp),%edx
   53d18:	mov    %rbx,%rsi
   53d1b:	mov    %r13,%rdi
   53d1e:	mov    (%rax),%r14
   53d21:	mov    0x8(%r13),%rax
   53d25:	mov    0x8(%rax),%rax
   53d29:	mov    0x8(%rax),%r15
   53d2d:	call   536c0 <clockcache_acquire_entry_for_load.lto_priv.0>
   53d32:	mov    %rax,%rbp
   53d35:	mov    $0xffffffff,%eax
   53d3a:	cmp    %rax,%rbp
   53d3d:	je     53c27 <clockcache_get+0x207>
   53d43:	mov    0x8(%r13),%rax
   53d47:	mov    %rbp,%r12
   53d4a:	mov    %r14,0x8(%rsp)
   53d4f:	mov    %r13,%r14
   53d52:	shl    $0x6,%r12
   53d56:	add    0x48(%r13),%r12
   53d5a:	mov    0x18(%rax),%ecx
   53d5d:	test   %ecx,%ecx
   53d5f:	jne    53f7e <clockcache_get+0x55e>
   53d65:	mov    0x18(%r14),%rdi
   53d69:	mov    (%r12),%rsi
   53d6d:	mov    %rbx,%rcx
   53d70:	mov    %r15,%rdx
   53d73:	mov    (%rdi),%rax
   53d76:	call   *(%rax)
   53d78:	mov    %eax,%r13d
   53d7b:	test   %eax,%eax
   53d7d:	jne    53fa5 <clockcache_get+0x585>
   53d83:	mov    0x8(%r14),%rax
   53d87:	mov    0x18(%rax),%edx
   53d8a:	test   %edx,%edx
   53d8c:	jne    53f21 <clockcache_get+0x501>
   53d92:	mov    %ebp,%edx
   53d94:	mov    %r13d,%ecx
   53d97:	mov    %rbx,%rsi
   53d9a:	mov    %r14,%rdi
   53d9d:	call   537f0 <clockcache_finish_load.lto_priv.0>
   53da2:	mov    %r12,%rdx
   53da5:	test   %r13d,%r13d
   53da8:	je     53baa <clockcache_get+0x18a>
   53dae:	mov    0x8(%r14),%rcx
   53db2:	mov    %rbp,%rax
   53db5:	xor    %edx,%edx
   53db7:	mov    0x8(%rsp),%rdi
   53dbc:	divq   0x140(%rcx)
   53dc3:	and    $0x3,%edi
   53dc6:	shl    $0x6,%edx
   53dc9:	lea    (%rdx,%rax,1),%esi
   53dcc:	mov    0x130(%rcx),%eax
   53dd2:	mov    0x88(%r14),%rdx
   53dd9:	mov    $0xffffffff,%ecx
   53dde:	imul   %rdi,%rax
   53de2:	add    %rsi,%rax
   53de5:	lock xadd %cx,(%rdx,%rax,2)
   53deb:	test   %cx,%cx
   53dee:	jne    53bcc <clockcache_get+0x1ac>
   53df4:	push   %rax
   53df5:	push   %rdi
   53df6:	push   %rsi
   53df7:	push   0x20(%rsp)
   53dfb:	xor    %r9d,%r9d
   53dfe:	lea    0x643f3(%rip),%r8        # b81f8 <__func__.21+0xd68>
   53e05:	xor    %eax,%eax
   53e07:	mov    $0x288,%esi
   53e0c:	lea    0x5d81b(%rip),%rcx        # b162e <_fini+0xa12>
   53e13:	lea    0x6ebf6(%rip),%rdx        # c2a10 <__func__.28.lto_priv.1>
   53e1a:	lea    0x5d7b6(%rip),%rdi        # b15d7 <_fini+0x9bb>
   53e21:	call   ee00 <platform_assert_false@plt>
   53e26:	cs nopw 0x0(%rax,%rax,1)
   53e30:	mov    0x81199(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53e37:	mov    %rbx,%r8
   53e3a:	xor    %eax,%eax
   53e3c:	mov    $0x164,%ecx
   53e41:	lea    0x6ea88(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   53e48:	lea    0x63659(%rip),%rsi        # b74a8 <__func__.21+0x18>
   53e4f:	mov    0x0(%rbp),%rdi
   53e53:	call   fbf0 <fprintf@plt>
   53e58:	mov    0x0(%rbp),%rdi
   53e5c:	call   fe90 <fflush@plt>
   53e61:	jmp    53cf9 <clockcache_get+0x2d9>
   53e66:	mov    0x10(%rsp),%rsi
   53e6b:	mov    0x1c(%rsp),%ecx
   53e6f:	lea    0x0(,%rsi,8),%rax
   53e77:	sub    %rsi,%rax
   53e7a:	lea    0x220(%rcx,%rax,8),%rax
   53e82:	addq   $0x1,(%r14,%rax,8)
   53e87:	jmp    53baa <clockcache_get+0x18a>
   53e8c:	mov    0x8(%r13),%rcx
   53e90:	xor    %edx,%edx
   53e92:	mov    0x10(%rsp),%rdi
   53e97:	divq   0x140(%rcx)
   53e9e:	and    $0x3,%edi
   53ea1:	shl    $0x6,%edx
   53ea4:	lea    (%rdx,%rax,1),%esi
   53ea7:	mov    0x130(%rcx),%eax
   53ead:	mov    0x88(%r13),%rdx
   53eb4:	mov    $0xffffffff,%ecx
   53eb9:	imul   %rdi,%rax
   53ebd:	add    %rsi,%rax
   53ec0:	lock xadd %cx,(%rdx,%rax,2)
   53ec6:	test   %cx,%cx
   53ec9:	jne    53c27 <clockcache_get+0x207>
   53ecf:	push   %rbx
   53ed0:	push   %rdi
   53ed1:	push   %rsi
   53ed2:	push   0x28(%rsp)
   53ed6:	jmp    53dfb <clockcache_get+0x3db>
   53edb:	nopl   0x0(%rax,%rax,1)
   53ee0:	sub    $0x8,%rsp
   53ee4:	mov    %rbx,%r9
   53ee7:	mov    %r15,%r8
   53eea:	mov    $0x172,%ecx
   53eef:	mov    0x810da(%rip),%rbp        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53ef6:	lea    0x6e9d3(%rip),%rdx        # c28d0 <__func__.22.lto_priv.0>
   53efd:	lea    0x635f4(%rip),%rsi        # b74f8 <__func__.21+0x68>
   53f04:	mov    0x0(%rbp),%rdi
   53f08:	push   $0x0
   53f0a:	call   fbf0 <fprintf@plt>
   53f0f:	mov    0x0(%rbp),%rdi
   53f13:	call   fe90 <fflush@plt>
   53f18:	pop    %r9
   53f1a:	pop    %r10
   53f1c:	jmp    53cf9 <clockcache_get+0x2d9>
   53f21:	lea    0x30(%rsp),%rsi
   53f26:	mov    $0x1,%edi
   53f2b:	call   e410 <clock_gettime@plt>
   53f30:	mov    0x28(%rsp),%rax
   53f35:	mov    0x8(%rsp),%rsi
   53f3a:	imul   $0x3b9aca00,0x30(%rsp),%rdx
   53f43:	add    0x38(%rsp),%rdx
   53f48:	sub    %rax,%rdx
   53f4b:	mov    0x1c(%rsp),%ecx
   53f4f:	lea    0x0(,%rsi,8),%rax
   53f57:	sub    %rsi,%rax
   53f5a:	lea    (%rcx,%rax,8),%rax
   53f5e:	lea    (%r14,%rax,8),%rax
   53f62:	addq   $0x1,0x1148(%rax)
   53f6a:	addq   $0x1,0x1220(%rax)
   53f72:	add    %rdx,0x1190(%rax)
   53f79:	jmp    53d92 <clockcache_get+0x372>
   53f7e:	lea    0x30(%rsp),%rsi
   53f83:	mov    $0x1,%edi
   53f88:	call   e410 <clock_gettime@plt>
   53f8d:	imul   $0x3b9aca00,0x30(%rsp),%rax
   53f96:	add    0x38(%rsp),%rax
   53f9b:	mov    %rax,0x28(%rsp)
   53fa0:	jmp    53d65 <clockcache_get+0x345>
   53fa5:	mov    %eax,%edi
   53fa7:	call   efe0 <strerror@plt>
   53fac:	mov    0x8101d(%rip),%r15        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   53fb3:	mov    0x1c(%rsp),%ecx
   53fb7:	mov    %rbx,%rdx
   53fba:	mov    %rax,%r8
   53fbd:	lea    0x648dc(%rip),%rsi        # b88a0 <__func__.21+0x1410>
   53fc4:	xor    %eax,%eax
   53fc6:	mov    (%r15),%rdi
   53fc9:	call   fbf0 <fprintf@plt>
   53fce:	mov    (%r15),%rdi
   53fd1:	call   fe90 <fflush@plt>
   53fd6:	mov    0x8(%r14),%rax
   53fda:	mov    0x18(%rax),%edx
   53fdd:	test   %edx,%edx
   53fdf:	je     53d92 <clockcache_get+0x372>
   53fe5:	jmp    53f21 <clockcache_get+0x501>
   53fea:	mov    0x8(%r14),%rcx
   53fee:	xor    %edx,%edx
   53ff0:	mov    0x10(%rsp),%rdi
   53ff5:	divq   0x140(%rcx)
   53ffc:	and    $0x3,%edi
   53fff:	shl    $0x6,%edx
   54002:	lea    (%rdx,%rax,1),%esi
   54005:	mov    0x130(%rcx),%eax
   5400b:	mov    0x88(%r14),%rdx
   54012:	mov    $0xffffffff,%ecx
   54017:	imul   %rdi,%rax
   5401b:	add    %rsi,%rax
   5401e:	lock xadd %cx,(%rdx,%rax,2)
   54024:	test   %cx,%cx
   54027:	jne    53bcc <clockcache_get+0x1ac>
   5402d:	jmp    53ecf <clockcache_get+0x4af>
   54032:	nopw   0x0(%rax,%rax,1)
   54038:	sub    $0x8,%rsp
   5403c:	lea    0x645fd(%rip),%r8        # b8640 <__func__.21+0x11b0>
   54043:	xor    %eax,%eax
   54045:	push   %rsi
   54046:	mov    0x2c(%rsp),%r9d
   5404b:	lea    0x64616(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   54052:	lea    0x6e847(%rip),%rdx        # c28a0 <__func__.23.lto_priv.1>
   54059:	mov    $0x1d6,%esi
   5405e:	lea    0x5d572(%rip),%rdi        # b15d7 <_fini+0x9bb>
   54065:	call   ee00 <platform_assert_false@plt>
   5406a:	mov    0x1c(%rsp),%esi
   5406e:	mov    %ebp,%r9d
   54071:	lea    0x5d692(%rip),%r8        # b170a <_fini+0xaee>
   54078:	lea    0x5d6a2(%rip),%rcx        # b1721 <_fini+0xb05>
   5407f:	lea    0x6e72a(%rip),%rdx        # c27b0 <__func__.19>
   54086:	lea    0x5d54a(%rip),%rdi        # b15d7 <_fini+0x9bb>
   5408d:	push   %rsi
   5408e:	mov    $0x8dd,%esi
   54093:	push   %rax
   54094:	xor    %eax,%eax
   54096:	call   ee00 <platform_assert_false@plt>
   5409b:	mov    %r13,%r14
   5409e:	jmp    1015d <clockcache_get.cold>

Disassembly of section .fini:
