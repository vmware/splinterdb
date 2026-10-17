
.cache/invalid-address-perf-20261017/builds/sequential/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000060d20 <core_range_iterator_next>:
   60d20:	push   %rbx
   60d21:	mov    0x43c(%rdi),%eax
   60d27:	test   %eax,%eax
   60d29:	je     60d52 <core_range_iterator_next+0x32>
   60d2b:	mov    %rdi,%rbx
   60d2e:	mov    0x430(%rdi),%rdi
   60d35:	mov    (%rdi),%rax
   60d38:	call   *0x18(%rax)
   60d3b:	test   %eax,%eax
   60d3d:	jne    60d50 <core_range_iterator_next+0x30>
   60d3f:	mov    %rbx,%rdi
   60d42:	pop    %rbx
   60d43:	jmp    60010 <core_range_iterator_next.part.0>
   60d48:	nopl   0x0(%rax,%rax,1)
   60d50:	pop    %rbx
   60d51:	ret
   60d52:	lea    0x50821(%rip),%r8        # b157a <_fini+0xa5e>
   60d59:	lea    0x50e98(%rip),%rcx        # b1bf8 <_fini+0x10dc>
   60d60:	xor    %eax,%eax
   60d62:	mov    $0x9f4,%esi
   60d67:	lea    0x62342(%rip),%rdx        # c30b0 <__func__.55>
   60d6e:	lea    0x50a28(%rip),%rdi        # b179d <_fini+0xc81>
   60d75:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
