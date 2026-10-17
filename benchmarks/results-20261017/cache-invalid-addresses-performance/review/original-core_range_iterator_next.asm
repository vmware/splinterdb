
.cache/invalid-address-perf-20261017/builds/original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

00000000000610a0 <core_range_iterator_next>:
   610a0:	push   %rbx
   610a1:	mov    0x43c(%rdi),%eax
   610a7:	test   %eax,%eax
   610a9:	je     610d2 <core_range_iterator_next+0x32>
   610ab:	mov    %rdi,%rbx
   610ae:	mov    0x430(%rdi),%rdi
   610b5:	mov    (%rdi),%rax
   610b8:	call   *0x18(%rax)
   610bb:	test   %eax,%eax
   610bd:	jne    610d0 <core_range_iterator_next+0x30>
   610bf:	mov    %rbx,%rdi
   610c2:	pop    %rbx
   610c3:	jmp    60390 <core_range_iterator_next.part.0>
   610c8:	nopl   0x0(%rax,%rax,1)
   610d0:	pop    %rbx
   610d1:	ret
   610d2:	lea    0x514a1(%rip),%r8        # b257a <_fini+0x145e>
   610d9:	lea    0x51b18(%rip),%rcx        # b2bf8 <_fini+0x1adc>
   610e0:	xor    %eax,%eax
   610e2:	mov    $0x9f4,%esi
   610e7:	lea    0x62fa2(%rip),%rdx        # c4090 <__func__.55>
   610ee:	lea    0x516a8(%rip),%rdi        # b279d <_fini+0x1681>
   610f5:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
