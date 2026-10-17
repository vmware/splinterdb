
.cache/invalid-address-perf-20261017/builds/outlined/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053c10 <clockcache_get>:
   53c10:	push   %r15
   53c12:	lea    -0x1(%rcx),%eax
   53c15:	push   %r14
   53c17:	push   %r13
   53c19:	push   %r12
   53c1b:	push   %rbp
   53c1c:	push   %rbx
   53c1d:	mov    %rsi,%rbx
   53c20:	sub    $0x48,%rsp
   53c24:	mov    %edx,0x8(%rsp)
   53c28:	mov    %ecx,0x1c(%rsp)
   53c2c:	mov    %r8,0x20(%rsp)
   53c31:	cmp    $0x7,%eax
   53c34:	ja     54158 <clockcache_get+0x548>
   53c3a:	mov    0x8(%rdi),%rax
   53c3e:	mov    %rdi,%r14
   53c41:	mov    %rsi,%rdi
   53c44:	xor    %edx,%edx
   53c46:	mov    0x8(%rax),%rsi
   53c4a:	mov    0x120(%rax),%rcx
   53c51:	mov    %rbx,%rax
   53c54:	divq   0x8(%rsi)
   53c58:	shr    %cl,%rdi
   53c5b:	mov    %rdi,%rcx
   53c5e:	test   %rdx,%rdx
   53c61:	jne    101b7 <clockcache_get.cold>
   53c67:	mov    0x28(%r14),%rax
   53c6b:	shr    $0x2,%rax
   53c6f:	cmp    %rax,%rdi
   53c72:	jae    101b7 <clockcache_get.cold>
   53c78:	movabs $0xcccccccccccccccd,%r12
   53c82:	mov    %r14,%r15
   53c85:	data16 cs nopw 0x0(%rax,%rax,1)
   53c90:	mov    0x30(%r15),%rax
   53c94:	mov    (%rax,%rcx,4),%ebp
   53c97:	cmp    $0xffffffff,%ebp
   53c9a:	je     53e60 <clockcache_get+0x250>
   53ca0:	data16 lea 0x81300(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53ca8:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53cb0:	mov    0x8(%rsp),%r9d
   53cb5:	mov    %ebp,%esi
   53cb7:	mov    %r15,%rdi
   53cba:	mov    (%rax),%rax
   53cbd:	mov    %rax,0x10(%rsp)
   53cc2:	test   %r9d,%r9d
   53cc5:	je     53e00 <clockcache_get+0x1f0>
   53ccb:	call   aa3a0 <clockcache_try_get_read.constprop.0>
   53cd0:	cmp    $0x1,%eax
   53cd3:	jne    53d1d <clockcache_get+0x10d>
   53cd5:	mov    $0x1,%r13d
   53cdb:	mov    %r13,%rax
   53cde:	mul    %r12
   53ce1:	xor    %eax,%eax
   53ce3:	shr    $0x2,%rdx
   53ce7:	mov    %rdx,%r14
   53cea:	nopw   0x0(%rax,%rax,1)
   53cf0:	pause
   53cf2:	mov    %rax,%rcx
   53cf5:	add    $0x1,%rax
   53cf9:	cmp    %rcx,%r14
   53cfc:	jne    53cf0 <clockcache_get+0xe0>
   53cfe:	cmp    $0x400,%r13
   53d05:	ja     53dd0 <clockcache_get+0x1c0>
   53d0b:	mov    %ebp,%esi
   53d0d:	mov    %r15,%rdi
   53d10:	add    %r13,%r13
   53d13:	call   aa3a0 <clockcache_try_get_read.constprop.0>
   53d18:	cmp    $0x1,%eax
   53d1b:	je     53cdb <clockcache_get+0xcb>
   53d1d:	test   %eax,%eax
   53d1f:	jne    53e13 <clockcache_get+0x203>
   53d25:	mov    %ebp,%eax
   53d27:	mov    0x48(%r15),%rcx
   53d2b:	mov    %rax,%rdx
   53d2e:	shl    $0x6,%rdx
   53d32:	mov    0x8(%rcx,%rdx,1),%rsi
   53d37:	cmp    %rsi,%rbx
   53d3a:	jne    53fe6 <clockcache_get+0x3d6>
   53d40:	mov    %r15,%r14
   53d43:	mov    %rax,%rbx
   53d46:	mov    %rdx,%r15
   53d49:	jmp    53d5c <clockcache_get+0x14c>
   53d4b:	nopl   0x0(%rax,%rax,1)
   53d50:	mov    %r14,%rdi
   53d53:	call   ea40 <clockcache_wait@plt>
   53d58:	mov    0x48(%r14),%rcx
   53d5c:	mov    0x10(%rcx,%r15,1),%eax
   53d61:	test   $0x10,%al
   53d63:	jne    53d50 <clockcache_get+0x140>
   53d65:	mov    0x48(%r14),%rdx
   53d69:	mov    %rbx,%rax
   53d6c:	add    %r15,%rdx
   53d6f:	mov    0x14(%rdx),%r13d
   53d73:	test   %r13d,%r13d
   53d76:	jne    54109 <clockcache_get+0x4f9>
   53d7c:	mov    0x20(%rdx),%eax
   53d7f:	cmp    %eax,0x1c(%rsp)
   53d83:	jne    5418a <clockcache_get+0x57a>
   53d89:	mov    0x8(%r14),%rax
   53d8d:	mov    0x18(%rax),%edi
   53d90:	test   %edi,%edi
   53d92:	jne    53fc0 <clockcache_get+0x3b0>
   53d98:	mov    0x20(%rsp),%rbx
   53d9d:	xor    %r13d,%r13d
   53da0:	mov    %rdx,(%rbx)
   53da3:	data16 lea 0x811fd(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53dab:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53db3:	mov    (%rax),%rax
   53db6:	mov    %rax,0x8(%rbx)
   53dba:	add    $0x48,%rsp
   53dbe:	mov    %r13d,%eax
   53dc1:	pop    %rbx
   53dc2:	pop    %rbp
   53dc3:	pop    %r12
   53dc5:	pop    %r13
   53dc7:	pop    %r14
   53dc9:	pop    %r15
   53dcb:	ret
   53dcc:	nopl   0x0(%rax)
   53dd0:	mov    %ebp,%esi
   53dd2:	mov    %r15,%rdi
   53dd5:	call   aa3a0 <clockcache_try_get_read.constprop.0>
   53dda:	cmp    $0x1,%eax
   53ddd:	jne    53d1d <clockcache_get+0x10d>
   53de3:	xor    %eax,%eax
   53de5:	data16 cs nopw 0x0(%rax,%rax,1)
   53df0:	pause
   53df2:	mov    %rax,%rdx
   53df5:	add    $0x1,%rax
   53df9:	cmp    %rdx,%r14
   53dfc:	jne    53df0 <clockcache_get+0x1e0>
   53dfe:	jmp    53dd0 <clockcache_get+0x1c0>
   53e00:	call   aa3a0 <clockcache_try_get_read.constprop.0>
   53e05:	cmp    $0x1,%eax
   53e08:	je     53e77 <clockcache_get+0x267>
   53e0a:	cmp    $0x2,%eax
   53e0d:	jne    53d25 <clockcache_get+0x115>
   53e13:	mov    0x8(%r15),%rax
   53e17:	xor    %edx,%edx
   53e19:	mov    %rbx,%rsi
   53e1c:	mov    0x8(%rax),%rdi
   53e20:	mov    0x120(%rax),%rcx
   53e27:	mov    %rbx,%rax
   53e2a:	divq   0x8(%rdi)
   53e2e:	shr    %cl,%rsi
   53e31:	mov    %rsi,%rcx
   53e34:	test   %rdx,%rdx
   53e37:	jne    541bb <clockcache_get+0x5ab>
   53e3d:	mov    0x28(%r15),%rax
   53e41:	shr    $0x2,%rax
   53e45:	cmp    %rax,%rsi
   53e48:	jb     53c90 <clockcache_get+0x80>
   53e4e:	mov    %r15,%r14
   53e51:	jmp    101b7 <clockcache_get.cold>
   53e56:	cs nopw 0x0(%rax,%rax,1)
   53e60:	mov    %rbx,%rsi
   53e63:	mov    %r15,%rdi
   53e66:	call   50100 <clockcache_page_valid_for_load.lto_priv.0>
   53e6b:	test   %eax,%eax
   53e6d:	je     53e88 <clockcache_get+0x278>
   53e6f:	mov    0x8(%rsp),%esi
   53e73:	test   %esi,%esi
   53e75:	jne    53e93 <clockcache_get+0x283>
   53e77:	mov    $0xb,%r13d
   53e7d:	jmp    53dba <clockcache_get+0x1aa>
   53e82:	nopw   0x0(%rax,%rax,1)
   53e88:	mov    $0x16,%r13d
   53e8e:	jmp    53dba <clockcache_get+0x1aa>
   53e93:	data16 lea 0x8110d(%rip),%rdi        # d4fa8 <xxxtid@@Base+0xd4fa8>
   53e9b:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53ea3:	mov    0x1c(%rsp),%edx
   53ea7:	mov    %rbx,%rsi
   53eaa:	mov    %r15,%rdi
   53ead:	mov    (%rax),%r13
   53eb0:	mov    0x8(%r15),%rax
   53eb4:	mov    0x8(%rax),%rax
   53eb8:	mov    0x8(%rax),%r14
   53ebc:	call   538b0 <clockcache_acquire_entry_for_load.lto_priv.0>
   53ec1:	mov    %rax,%rbp
   53ec4:	mov    $0xffffffff,%eax
   53ec9:	cmp    %rax,%rbp
   53ecc:	je     53e13 <clockcache_get+0x203>
   53ed2:	mov    %r15,%rax
   53ed5:	mov    %rbp,%r12
   53ed8:	mov    %r14,%r15
   53edb:	mov    %r13,0x8(%rsp)
   53ee0:	mov    %rax,%r14
   53ee3:	shl    $0x6,%r12
   53ee7:	add    0x48(%rax),%r12
   53eeb:	mov    0x8(%rax),%rax
   53eef:	mov    0x18(%rax),%ecx
   53ef2:	test   %ecx,%ecx
   53ef4:	jne    5409d <clockcache_get+0x48d>
   53efa:	mov    0x18(%r14),%rdi
   53efe:	mov    (%r12),%rsi
   53f02:	mov    %rbx,%rcx
   53f05:	mov    %r15,%rdx
   53f08:	mov    (%rdi),%rax
   53f0b:	call   *(%rax)
   53f0d:	mov    %eax,%r13d
   53f10:	test   %eax,%eax
   53f12:	jne    540c4 <clockcache_get+0x4b4>
   53f18:	mov    0x8(%r14),%rax
   53f1c:	mov    0x18(%rax),%edx
   53f1f:	test   %edx,%edx
   53f21:	jne    54040 <clockcache_get+0x430>
   53f27:	mov    %ebp,%edx
   53f29:	mov    %r13d,%ecx
   53f2c:	mov    %rbx,%rsi
   53f2f:	mov    %r14,%rdi
   53f32:	call   539e0 <clockcache_finish_load.lto_priv.0>
   53f37:	mov    %r12,%rdx
   53f3a:	test   %r13d,%r13d
   53f3d:	je     53d98 <clockcache_get+0x188>
   53f43:	mov    0x8(%r14),%rcx
   53f47:	mov    %rbp,%rax
   53f4a:	xor    %edx,%edx
   53f4c:	mov    0x8(%rsp),%rdi
   53f51:	divq   0x140(%rcx)
   53f58:	and    $0x3,%edi
   53f5b:	shl    $0x6,%edx
   53f5e:	lea    (%rdx,%rax,1),%esi
   53f61:	mov    0x130(%rcx),%eax
   53f67:	mov    0x88(%r14),%rdx
   53f6e:	mov    $0xffffffff,%ecx
   53f73:	imul   %rdi,%rax
   53f77:	add    %rsi,%rax
   53f7a:	lock xadd %cx,(%rdx,%rax,2)
   53f80:	test   %cx,%cx
   53f83:	jne    53dba <clockcache_get+0x1aa>
   53f89:	push   %rax
   53f8a:	push   %rdi
   53f8b:	push   %rsi
   53f8c:	push   0x20(%rsp)
   53f90:	xor    %r9d,%r9d
   53f93:	lea    0x6425e(%rip),%r8        # b81f8 <__func__.21+0xd68>
   53f9a:	xor    %eax,%eax
   53f9c:	mov    $0x28c,%esi
   53fa1:	lea    0x5d686(%rip),%rcx        # b162e <_fini+0xa52>
   53fa8:	lea    0x6ea21(%rip),%rdx        # c29d0 <__func__.28.lto_priv.1>
   53faf:	lea    0x5d621(%rip),%rdi        # b15d7 <_fini+0x9fb>
   53fb6:	call   ee00 <platform_assert_false@plt>
   53fbb:	nopl   0x0(%rax,%rax,1)
   53fc0:	mov    0x10(%rsp),%rbx
   53fc5:	mov    0x1c(%rsp),%ecx
   53fc9:	lea    0x0(,%rbx,8),%rax
   53fd1:	sub    %rbx,%rax
   53fd4:	lea    0x220(%rcx,%rax,8),%rax
   53fdc:	addq   $0x1,(%r14,%rax,8)
   53fe1:	jmp    53d98 <clockcache_get+0x188>
   53fe6:	mov    0x8(%r15),%rcx
   53fea:	xor    %edx,%edx
   53fec:	mov    0x10(%rsp),%rdi
   53ff1:	divq   0x140(%rcx)
   53ff8:	and    $0x3,%edi
   53ffb:	shl    $0x6,%edx
   53ffe:	lea    (%rdx,%rax,1),%esi
   54001:	mov    0x130(%rcx),%eax
   54007:	mov    0x88(%r15),%rdx
   5400e:	mov    $0xffffffff,%ecx
   54013:	imul   %rdi,%rax
   54017:	add    %rsi,%rax
   5401a:	lock xadd %cx,(%rdx,%rax,2)
   54020:	test   %cx,%cx
   54023:	jne    53e13 <clockcache_get+0x203>
   54029:	push   %r8
   5402b:	push   %rdi
   5402c:	push   %rsi
   5402d:	push   0x28(%rsp)
   54031:	jmp    53f90 <clockcache_get+0x380>
   54036:	cs nopw 0x0(%rax,%rax,1)
   54040:	mov    $0x1,%edi
   54045:	lea    0x30(%rsp),%rsi
   5404a:	call   e410 <clock_gettime@plt>
   5404f:	mov    0x28(%rsp),%rax
   54054:	mov    0x8(%rsp),%rdi
   54059:	imul   $0x3b9aca00,0x30(%rsp),%rdx
   54062:	add    0x38(%rsp),%rdx
   54067:	sub    %rax,%rdx
   5406a:	mov    0x1c(%rsp),%ecx
   5406e:	lea    0x0(,%rdi,8),%rax
   54076:	sub    %rdi,%rax
   54079:	lea    (%rcx,%rax,8),%rax
   5407d:	lea    (%r14,%rax,8),%rax
   54081:	addq   $0x1,0x1148(%rax)
   54089:	addq   $0x1,0x1220(%rax)
   54091:	add    %rdx,0x1190(%rax)
   54098:	jmp    53f27 <clockcache_get+0x317>
   5409d:	lea    0x30(%rsp),%rsi
   540a2:	mov    $0x1,%edi
   540a7:	call   e410 <clock_gettime@plt>
   540ac:	imul   $0x3b9aca00,0x30(%rsp),%rax
   540b5:	add    0x38(%rsp),%rax
   540ba:	mov    %rax,0x28(%rsp)
   540bf:	jmp    53efa <clockcache_get+0x2ea>
   540c4:	mov    %eax,%edi
   540c6:	call   efe0 <strerror@plt>
   540cb:	mov    0x80efe(%rip),%r15        # d4fd0 <Platform_error_log_handle@@Base-0x1368>
   540d2:	mov    0x1c(%rsp),%ecx
   540d6:	mov    %rbx,%rdx
   540d9:	mov    %rax,%r8
   540dc:	lea    0x647bd(%rip),%rsi        # b88a0 <__func__.21+0x1410>
   540e3:	xor    %eax,%eax
   540e5:	mov    (%r15),%rdi
   540e8:	call   fbf0 <fprintf@plt>
   540ed:	mov    (%r15),%rdi
   540f0:	call   fe90 <fflush@plt>
   540f5:	mov    0x8(%r14),%rax
   540f9:	mov    0x18(%rax),%edx
   540fc:	test   %edx,%edx
   540fe:	je     53f27 <clockcache_get+0x317>
   54104:	jmp    54040 <clockcache_get+0x430>
   54109:	mov    0x8(%r14),%rcx
   5410d:	xor    %edx,%edx
   5410f:	mov    0x10(%rsp),%rdi
   54114:	divq   0x140(%rcx)
   5411b:	and    $0x3,%edi
   5411e:	shl    $0x6,%edx
   54121:	lea    (%rdx,%rax,1),%esi
   54124:	mov    0x130(%rcx),%eax
   5412a:	mov    0x88(%r14),%rdx
   54131:	mov    $0xffffffff,%ecx
   54136:	imul   %rdi,%rax
   5413a:	add    %rsi,%rax
   5413d:	lock xadd %cx,(%rdx,%rax,2)
   54143:	test   %cx,%cx
   54146:	jne    53dba <clockcache_get+0x1aa>
   5414c:	jmp    54029 <clockcache_get+0x419>
   54151:	nopl   0x0(%rax)
   54158:	sub    $0x8,%rsp
   5415c:	lea    0x644dd(%rip),%r8        # b8640 <__func__.21+0x11b0>
   54163:	xor    %eax,%eax
   54165:	push   %rsi
   54166:	mov    0x2c(%rsp),%r9d
   5416b:	lea    0x644f6(%rip),%rcx        # b8668 <__func__.21+0x11d8>
   54172:	lea    0x6e707(%rip),%rdx        # c2880 <__func__.23.lto_priv.1>
   54179:	mov    $0x1da,%esi
   5417e:	lea    0x5d452(%rip),%rdi        # b15d7 <_fini+0x9fb>
   54185:	call   ee00 <platform_assert_false@plt>
   5418a:	mov    0x1c(%rsp),%ebx
   5418e:	mov    %ebp,%r9d
   54191:	lea    0x5d572(%rip),%r8        # b170a <_fini+0xb2e>
   54198:	lea    0x5d582(%rip),%rcx        # b1721 <_fini+0xb45>
   5419f:	lea    0x6e5ea(%rip),%rdx        # c2790 <__func__.19>
   541a6:	mov    $0x8e1,%esi
   541ab:	lea    0x5d425(%rip),%rdi        # b15d7 <_fini+0x9fb>
   541b2:	push   %rbx
   541b3:	push   %rax
   541b4:	xor    %eax,%eax
   541b6:	call   ee00 <platform_assert_false@plt>
   541bb:	mov    %r15,%r14
   541be:	jmp    101b7 <clockcache_get.cold>

Disassembly of section .fini:
